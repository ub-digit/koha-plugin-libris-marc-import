package Koha::Plugin::Se::Ub::Gu::MarcImport;

use Modern::Perl;
use Encode qw(encode);
## Required for all plugins
use base qw(Koha::Plugins::Base);

#TODO: Framework argument, should use existing framework for exising records, and fallback to the provided one

## We will also need to include any Koha libraries we want to access
use C4::Context;
use C4::Biblio;
use C4::Items;
use C4::Search;
use C4::Charset qw(SetUTF8Flag);
#TODO Try removing this, probably not needed
#use Koha qw(ItemTypes);
#use Koha::Itemtypes;
use Koha::Libraries;
use Koha::SearchEngine;
use Koha::SearchEngine::Search;
use Koha::BiblioFrameworks;
# use Koha::Logger;
use Data::Dumper;
use MARC::Record;
use MARC::Batch;
use MARC::File::USMARC;
use MARC::File::XML; #TODO: Sniff xml of incoming data and support this
# use MARC::Charset; #?
use List::MoreUtils qw(all any);
use Unicode::Normalize qw(normalize check);
use Koha::Exceptions;
use Digest::MD5 qw(md5_hex);
use POSIX qw(strftime);
use File::Path qw(make_path);

## Here we set our plugin version
our $VERSION = 0.01;

## Here is our metadata, some keys are required, some are optional
our $metadata = {
    name   => 'GUB Marc Import Plugin',
    author => 'David Gustafsson',
    description => 'Custom marc import tweaks (@todo: proper desc)',
    date_authored   => '2017-02-07',
    date_updated    => '2017-09-04',
    minimum_version => '16.05',
    maximum_version => undef,
    version         => $VERSION,
};

## This is the minimum code required for a plugin's 'new' method
## More can be added, but none should be removed
sub new {
    my ( $class, $args ) = @_;

    ## We need to add our metadata here so our base class can access it
    $args->{'metadata'} = $metadata;
    $args->{'metadata'}->{'class'} = $class;

    ## Here, we call the 'new' method for our base class
    ## This runs some additional magic and checking
    ## and returns our actual $self
    my $self = $class->SUPER::new($args);

    return $self;
}

## The existiance of a 'to_marc' subroutine means the plugin is capable
## of converting some type of file to MARC for use from the stage records
## for import tool
##
#
# 1. Move incomoing control number (TODO: move this down)
# 2. Apply matching rules using search engine to find potential match
# 3. If match found, insert koha id into incoming record
# 4. Apply deduplication logic
# 5. Process items
sub to_marc {
    my ( $self, $args ) = @_;

    my $debug = $args->{debug};
    #my $logger = Koha::Logger->get;

    my @marc_records = ();
    my $marc_record = undef;
    my $marc_type = C4::Context->preference('marcflavour');

    my $marc_batch;
    my $fh;
    {
        open($fh, "<", \$args->{data});
        binmode $fh, ':raw';
        $marc_batch = MARC::Batch->new('USMARC', $fh);
    }

    my @processed_marc_records;

    my @valid_utf8_normalization_forms = ('D', 'C', 'KD', 'KC');
    #my %valid_item_types = %{ ItemTypes() };


    # @FIXME: our?
    my $default_incoming_record_items_tag = '949';
    my $default_normalize_utf8_normalization_form = 'C';

    my $config = {
        framework => $self->retrieve_data('framework') // '',
        process_incoming_record_items_enable  => $self->retrieve_data('process_incoming_record_items_enable') || '0',
        incoming_record_items_tag  => $self->retrieve_data('incoming_record_items_tag') // $default_incoming_record_items_tag,
        normalize_utf8_enable => $self->retrieve_data('normalize_utf8_enable') || '0',
        normalize_utf8_normalization_form => $self->retrieve_data('normalize_utf8_normalization_form') // $default_normalize_utf8_normalization_form,
        deduplicate_fields_tagspecs => [split(/[\r\n]+/, $self->retrieve_data('deduplicate_fields_tagspecs') // '')], # '035a' # @FIXME tagspec/fieldspec pick one
        deduplicate_fields_enable => $self->retrieve_data('deduplicate_fields_enable') || '0',
        move_incoming_control_number_enable => $self->retrieve_data('move_incoming_control_number_enable') || '0',
        record_matching_enable => $self->retrieve_data('record_matching_enable') || '0',
        matchpoints => [split(/[\r\n]+/, $self->retrieve_data('matchpoints') // '')], # 'system-control-number,035a'
        protect_authority_linkage_enable => $self->retrieve_data('protect_authority_linkage_enable') || '0',
    };

    #TODO: setting
    my $authorities = 0;
    my $server = ($authorities ? 'authorityserver' : 'biblioserver');
    my $searcher = Koha::SearchEngine::Search->new(
        {
            index => (
                $authorities
                ? $Koha::SearchEngine::AUTHORITIES_INDEX
                : $Koha::SearchEngine::BIBLIOS_INDEX
            )
        }
    );

    # Canonical way of getting internal koha tagspec?
    my ($koha_local_id_tag, $koha_local_id_subfield) = GetMarcFromKohaField('biblio.biblionumber', $config->{framework});
    my ($koha_items_tag) = GetMarcFromKohaField('items.itemnumber', $config->{framework});

    my $i = -1;
    RECORD: while(1) {
        $i++;
        my $record;
        # Infinite loop and eval so can catch marc decode/parsing errors
        eval { $record = $marc_batch->next() };
        if ($@) {
            # \x1D is end of record
            # TODO: Think record always undef if errors occured
            # so this check can probably be removed
            my $record_data = undef;
            if (defined $record) {
                $record_data = $record->as_usmarc;
            }
            else {
                my @records_raw = split("\x1D", $args->{data});
                $record_data = $records_raw[$i];
            }
            $self->_stashFailedMarcRecord($record_data, "invalid_record", "Failed to parse MARC record: $@");
            next;
        }
        elsif(!$record) {
            last;
        }

        my $matched_record_id = undef; # Can remove = undef?
        my $koha_local_record_id = $record->subfield($koha_local_id_tag, $koha_local_id_subfield);
        if (defined($koha_local_record_id) && GetBiblio($koha_local_record_id)) {
            # Probably from other koha instance, or some other issue
            # Koha::Exceptions::Object::Exception->throw("Koha local id set by no matching record found");
            # What to do?
            $matched_record_id = $koha_local_record_id;
        }

        # @TODO: move this later in execution?
        ## Move incoming control number
        if ($config->{move_incoming_control_number_enable}) {
            my $control_number_identifier_field = $record->field('003');
            my $control_number = $record->field('001');
            if ($control_number_identifier_field && $control_number) {
                my $system_control_number = '(' . $control_number_identifier_field->data . ')' . $record->field('001')->data;
                my @record_system_control_numbers = $record->subfield('035', 'a');
                # Add if not already present
                if(!(@record_system_control_numbers && (any { $_ eq $system_control_number } @record_system_control_numbers))) {
                    $record->insert_fields_ordered(MARC::Field->new('035', ' ', ' ', 'a' => $system_control_number));
                }
            }
        }

        ## Perform search engine based record matching
        if (!$matched_record_id && $config->{record_matching_enable}) {
            SEQUENTIAL_MATCH: foreach my $matchpoint (@{$config->{matchpoints}}) {
                my $query = build_simplequery($matchpoint, $record, 'OR');
                if ($query) {
                    my ($error, $results, $totalhits) = $searcher->simple_search_compat($query, 0, 3, [$server]);
                    if (defined $error) {
                        #Koha::Exceptions::Object::Exception->throw("Search for matching record resulted in error: $error");
                        # TODO: Write to log instead
                        die("Search for matching record resulted in error: $error");
                    }
                    $results //= [];

                    if (@{$results} == 1) {
                        foreach my $result (@{$results}) {
                            my $_record = C4::Search::new_record_from_zebra($server, $result);
                            SetUTF8Flag($_record); # From bulkmarcimport, can remove this? Probably
                            $matched_record_id = $_record->subfield($koha_local_id_tag, $koha_local_id_subfield);
                        }
                        last SEQUENTIAL_MATCH;
                    }
                    elsif (@{$results} > 1) {
                        # @TODO: Should perhaps add both records in 999c instead and let downstream take care of duplicates
                        my $error = "More than one match for $query";
                        warn $error;
                        $self->_stashFailedMarcRecord($record->as_usmarc(), "multiple_matches", $error);
                    }
                    else {
                        # @TODO: Really warn here?
                        warn "No match for $query";
                    }
                }
            }
        }

        ## Handle record deletion by status flag
        if (marc_record_deleted($record)) {
            if ($matched_record_id) {
                my $error = DelBiblio($matched_record_id);
                if ($error) {
                    die("ERROR: Delete biblio $matched_record_id failed: $error\n");
                }
            }
            else {
                # @TODO: Throw exception instead? We really want to notify the user about this,
                # also include record identification number
                warn "Record marked for deletion, but no matching record found";
            }
        }
        else {
            if ($config->{normalize_utf8_enable}) {
                if(any { $_ eq $config->{normalize_utf8_normalization_form} } @valid_utf8_normalization_forms) {
                    # @FIXME: $format paramter?
                    my $record_xml = $record->as_xml();
                    # Only convert if we really have to, perhaps remove this overly zealous optimization
                    # as it might also slow things down (if it is almost never the case that record already
                    # has the desired normalization form)
                    if(!check($config->{normalize_utf8_normalization_form}, $record_xml)) {
                        my $record_normalized_xml = normalize($config->{normalize_utf8_normalization_form}, $record_xml);
                        $record = MARC::Record->new_from_xml($record_normalized_xml, 'UTF-8');
                    }
                }
                else {
                    die("Invalid UTF-8 normalization form: $config->{normalize_utf8_normalization_form}");
                }
            }
            ## Dedup incoming record field
            if ($config->{deduplicate_fields_enable}) {
                foreach my $field_spec (@{$config->{deduplicate_fields_tagspecs}}) {
                    my ($tag_spec, $subfield) = $field_spec =~ /([0-9.]{3})([a-zA-Z0-9])/;
                    if (!$tag_spec) {
                        die "Empty or invalid tag-spec: \"$tag_spec\"";
                    }
                    foreach my $tag (marc_record_tag_spec_expand_tags($record, $tag_spec)) {
                        in_place_dedup_record_field($record, $tag, $subfield);
                    }
                }
            }
            ## Set koha local id if we got match
            if ($matched_record_id) {
                # Remove old koha field if present
                my @local_id_fields = $record->field($koha_local_id_tag);
                if (@local_id_fields) {
                    $record->delete_fields(@local_id_fields);
                }
                # Set matched id as local id
                my $local_id_field = MARC::Field->new($koha_local_id_tag, '', '', $koha_local_id_subfield => $matched_record_id);
                $record->insert_fields_ordered($local_id_field);
            }

            if ($config->{process_incoming_record_items_enable}) {
                _processIncomingRecordItems(
                    $record,
                    $matched_record_id,
                    $config->{incoming_record_items_tag},
                    $koha_items_tag,
                    $config->{framework},
                    $debug
                );
            }
            push @processed_marc_records, $record;
        }
    }
    if ($config->{protect_authority_linkage_enable}) {
        my $dbh = C4::Context->dbh;
        # TODO: Could make field name configurable, and muliple values allowed
        my $field_specs = $dbh->selectcol_arrayref(qq{
            SELECT `marc_field` FROM `search_marc_map` JOIN `search_marc_to_field`
                ON `search_marc_map`.`id` = `search_marc_to_field`.`search_marc_map_id`
            JOIN `search_field`
                ON `search_marc_to_field`.`search_field_id` = `search_field`.`id`
            WHERE `search_field`.`name` = ? AND `search_marc_map`.`marc_type` = ?
        }, undef, 'koha-auth-number', $marc_type);
        _pruneMarcRecords($field_specs, \@processed_marc_records);
    }
    close($fh); # This can probably be omitted, no point in closing file handle to in memory variable?
    return encode('UTF-8', join("", map { $_->as_usmarc() } @processed_marc_records), 1);
}

# Libris => Koha mapping
# @TODO(!) this is now hard coded, should feted from koha items fields mappings
# my $mss = GetMarcSubfieldStructure($frameworkcode);
#
# $D 9 siffror => $c LOC 6 siffror (ta bort de tre inledande nollorna)
# $F => $t
# $X => $y
# $6 => $p
# $a => $o
# $s => Statuskod
# $t => {YN}N{YN}NN
# 1:a {YN} == Rq == Får beställas
# 2:a {YN} == Om Y använd $K för lånetid
# $K => Lånetid

# TODO: add 't' and 'K'!!
use constant {
    LIBRIS_ITEM_COPYNUMBER => 'F',
    LIBRIS_ITEM_TYPE => 'X',
    LIBRIS_ITEM_BARCODE => '6',
    LIBRIS_ITEM_CALLNUMBER => 'a',
    LIBRIS_ITEM_NOT_FOR_LOAN => 's',
    LIBRIS_ITEM_LOCATION => 'D',
    LIBRIS_ITEM_NOTE => 'z',
};

# => will automatically quite word before, use "," instead
# since constants will not be evaluated otherwise
my %libris_koha_subfield_mappings = (
    LIBRIS_ITEM_COPYNUMBER, 't', #t: items.copynumber
    LIBRIS_ITEM_TYPE, 'y', #y: items.itype
    LIBRIS_ITEM_BARCODE, 'p', #p: items.barcode
    LIBRIS_ITEM_CALLNUMBER, 'o', #o: items.itemcallnumber
    LIBRIS_ITEM_NOT_FOR_LOAN, '7' #7: Not for loan
    #'t' => ??
    #'K' => ??,
);
# Properties for which if all equal two items considered equal
my @item_field_comparison_subfields = (
    LIBRIS_ITEM_COPYNUMBER,
    LIBRIS_ITEM_TYPE,
    LIBRIS_ITEM_LOCATION, # includes homebranch
    LIBRIS_ITEM_BARCODE, # Hmm, this can been removed, should have been checked against earlier?
    LIBRIS_ITEM_CALLNUMBER,
    LIBRIS_ITEM_NOTE, #item note so we know is from libris (magic marker)
);

sub _validBranchcodes {
    state $valid_branchcodes;
    unless ($valid_branchcodes) {
        my $branches = Koha::Libraries->search({}, {});
        while (my $row = $branches->next) {
            $valid_branchcodes->{$row->get_column('branchcode')} = 1;
        }
    }
    return $valid_branchcodes;
}

sub _processIncomingRecordItems {
    my (
        $record,
        $matched_record_id,
        $incoming_record_items_tag,
        $koha_items_tag,
        $marc_framework,
        $debug
    ) = @_;

    my @existing_libris_item_fields;
    my @new_koha_item_fields;
    my $existing_koha_items = undef;
    my $valid_branchcodes = _validBranchcodes();

    my @incoming_item_fields = $record->field($incoming_record_items_tag);
    # If no incoming records nothing needs to be done
    return unless @incoming_item_fields;

    if ($matched_record_id) {
        my $matched_record = GetMarcBiblio({biblionumber => $matched_record_id});
        @existing_libris_item_fields = $matched_record->field($incoming_record_items_tag);
    }

    # Summary of what is happening here:
    #
    # For each incoming field item (from libris):
    #
    # 1. Set "z" (item notes) magic marker to mark item as originating from libris
    # 2. If incoming item has barcode, discard item if item with this barcode exists in Koha
    # 3. If record has a match, and matched record contains libris items:
    #   - Match record based on @item_field_comparison_subfields
    #   - If all subfields matches, incoming item is concidered a duplicate and is discarded
    #   - Else we have a possible new item
    # 4. Unless item was discared, construct a new Koha item from incoming subfield values
    # 5. If existing record id was provided, load existing record Koha items
    #   - If any existing koha item has same location as incoming item, discard item
    # 6. Unless item was discared, add new item as Koha item field to incoming record

    # @FIXME: rename incoming record items tag to libris items tag or similar
    INCOMING_ITEM_FIELD: foreach my $incoming_item_field (@incoming_item_fields) {

        # This will set note to '1' in $record since this is an object (reference)
        $incoming_item_field->update(LIBRIS_ITEM_NOTE, '1');
        print "\n*** Incoming item ***\n ${\$incoming_item_field->as_formatted()} \n\n" if $debug;

        # First globally check for possibly existing barcodes
        if (
            $incoming_item_field->subfield(LIBRIS_ITEM_BARCODE) &&
            GetItemnumberFromBarcode($incoming_item_field->subfield(LIBRIS_ITEM_BARCODE))
        ) {
            print "Found existing barcode (${\$incoming_item_field->subfield(LIBRIS_ITEM_BARCODE)}), skipping\n" if $debug;
            # There exists an item with same barcode as incoming item, skip
            next INCOMING_ITEM_FIELD;
        }
        if (@existing_libris_item_fields) {
            foreach my $existing_libris_item_field (@existing_libris_item_fields) {
                if ($debug) {
                    print "Matching fields:\n";
                    foreach my $subfield (@item_field_comparison_subfields) {
                        if (!$incoming_item_field->subfield($subfield)) {
                            print "Subfield \"$subfield\" not set for incoming item.\n" if $debug;
                        }
                        if (!$existing_libris_item_field->subfield($subfield)) {
                            print "Subfield \"$subfield\" not set for existing item.\n" if $debug;
                        }
                        if ($incoming_item_field->subfield($subfield) eq $existing_libris_item_field->subfield($subfield)) {
                            print "$subfield (${\$existing_libris_item_field->subfield($subfield)})\n";
                        }
                    }
                    if( all { $incoming_item_field->subfield($_) eq $existing_libris_item_field->subfield($_)  } @item_field_comparison_subfields ) {
                        print "All properties match\n";
                    }
                }
                if (
                    # If item comparison properties match, and incoming item has been marked
                    # as previously added, skip
                    (all { $incoming_item_field->subfield($_) eq $existing_libris_item_field->subfield($_) } @item_field_comparison_subfields)
                ) {
                    print "Properties match, skipping\n" if $debug;
                    next INCOMING_ITEM_FIELD;
                }
            }
        }
        my %koha_subfield_values = ();
        my $data;

        # First process all direct mappings
        foreach my $libris_subfield (keys %libris_koha_subfield_mappings) {
            $data = $incoming_item_field->subfield($libris_subfield);
            if (defined $data) {
                $koha_subfield_values{ $libris_koha_subfield_mappings{ $libris_subfield } } = $data;
            }
        }
        # Special cases:
        $data = $incoming_item_field->subfield(LIBRIS_ITEM_LOCATION);

        #if (length($data) != 9) {
        #    my $msg = "Invalid 'D' subfield value \"$data\" in incoming item, skipping";
        #    warn $msg;
        #    print $msg if $debug;
        #    next INCOMING_ITEM_FIELD;
        #}
        # TODO: Fetch item mappings from Koha?
        $koha_subfield_values{'c'} = substr $data, -6; #c: items.location
        $koha_subfield_values{'a'} = substr $data, -6, 2; #a: items.homebranch
        $koha_subfield_values{'b'} = substr $data, -6, 2; #a: items.holdingbranch

        if (!$valid_branchcodes->{$koha_subfield_values{'a'}}) {
            my $msg = "Invalid branchcode \"${\$koha_subfield_values{'a'}}\" in incoming item, skipping";
            warn $msg;
            print $msg if $debug;
            next INCOMING_ITEM_FIELD;
        }

        if (%koha_subfield_values) {
            # Lazy load existing koha items
            if (!(defined $existing_koha_items) && $matched_record_id) {
                $existing_koha_items = [];
                my $existing_koha_itemnumbers = GetItemnumbersForBiblio($matched_record_id);
                foreach my $itemnumber (@{$existing_koha_itemnumbers}) {
                    push @{$existing_koha_items}, GetItem($itemnumber);
                }
            }
            my $incoming_koha_item_field = MARC::Field->new($koha_items_tag, '', '', %koha_subfield_values);
            # Construct Koha item from incoming item values
            my $incoming_koha_item = GetKohaItemsFromMarcField($incoming_koha_item_field, $marc_framework);
            # If identical shelving locations the two items are considered equal, skip
            if (any { $incoming_koha_item->{'location'} eq $_->{'location'} } @{$existing_koha_items}) {
                print "Location \"${\$incoming_koha_item->{'location'}}\" matches existing item, skipping\n" if $debug;
                next INCOMING_ITEM_FIELD;
            }
            # Add to new items batch to be added
            print "New item:\n ${\Dumper($incoming_koha_item)}\n\n" if $debug;
            push @new_koha_item_fields, $incoming_koha_item_field;
        }
    }
    if (@new_koha_item_fields) {
        $record->insert_fields_ordered(@new_koha_item_fields);
    }
}

sub _pruneMarcRecords {
    my ($field_specs_prune, $records) = @_;
    my %prune_fields;
    my $field_regex = qr/([0-9]{3})([a-zA-Z0-9])*/;
    foreach my $field_spec (@{$field_specs_prune}) {
        my ($tag, $subfields) = $field_spec =~ $field_regex;
        if ($tag) {
            $prune_fields{$tag} = $subfields ? [split(//, $subfields)] : undef;
        }
    }
    my @fields_delete;
    foreach my $record (@{$records}) {
        @fields_delete = ();
        foreach my $field ($record->fields()) {
            my $tag = $field->tag();
            if (exists $prune_fields{$tag}) {
                my $subfields = $prune_fields{$tag};
                if ($subfields) {
                    $field->delete_subfield(code => $subfields);
                }
                else {
                    push @fields_delete, $field;
                }
            }
        }
        if (@fields_delete) {
            $record->delete_fields(@fields_delete);
        }
    }
}

# TODO: underscore private methods/subs?

sub GetKohaItemsFromMarcField {
    my ($item_field, $frameworkcode) = @_;
    my $temp_item_marc = MARC::Record->new();
    $temp_item_marc->append_fields($item_field);
    return TransformMarcToKoha($temp_item_marc, $frameworkcode, 'items');
}

sub GetKohaItemsFromMarcRecord {
    #TODO: Default value '' for frameworkcode?
    my ($record, $frameworkcode) = @_;
    my ($item_tag, $item_subfield) = GetMarcFromKohaField('items.itemnumber', $frameworkcode);
    my @koha_items;
    ITEMFIELD: foreach my $item_field ($record->field($item_tag)) {
        my $item = GetKohaItemsFromMarcField($item_field, $frameworkcode);
        # TODO: Skip these for now, since private subs in Items.pm
        # my $unlinked_item_subfields = _get_unlinked_item_subfields($temp_item_marc, $frameworkcode);
        # $item->{'more_subfields_xml'} = _get_unlinked_subfields_xml($unlinked_item_subfields);
        # my ($koha_biblionumber_tag, $koha_biblionumber_subfield) = GetMarcFromKohaField('biblio.biblionumber', $frameworkcode);
        # my $biblionumber = $record->subfield(GetMarcFromKohaField('biblio.biblionumber', $frameworkcode));
        # my ($koha_biblioitemnumber_tag, $koha_bibliotitemnumber_subfield) = GetMarcFromKohaField('biblio.biblioitemnumber', $frameworkcode);
        # my $biblioitemnumber = $record->subfield(GetMarcFromKohaField('biblio.biblioitemnumber', $frameworkcode));
        # $item->{'biblionumber'} = $biblionumber;
        # $item->{'biblioitemnumber'} = $biblioitemnumber;
        push @koha_items, $item;
    }
    return @koha_items;
}

sub _stashFailedMarcRecord {
    my ($self, $record_data, $error_type, $error_msg) = @_;
    my $directory = $self->retrieve_data('stash_failed_records_directory');
    if ($self->retrieve_data('stash_failed_records_enable') && length($directory)) {
        # Create error type directory if not exists
        my $output_dir = "$directory/$error_type";
        # TODO: error if failed to create
        make_path("$output_dir", {
            chmod => 0777,
            error => \my $err
        });
        if (@$err) {
            die("Unable to create $output_dir");
        }
        my $record_hash = md5_hex($record_data);
        my $date = strftime "%d-%m-%Y", localtime;
        my $fh;
        # TODO: Possible UTF-8 encoding woes, is this ok?
        # TODO: Hmm, probably will fail with raw records vs from ->as_usmarc??
        my $filename = "$output_dir/$date.$record_hash.marc";
        open($fh, ">", $filename) or die("Can't open $filename for writing!");
        print $fh $record_data;
        close($fh);
        $filename = "$output_dir/$date.$record_hash.error";
        open($fh, ">", $filename) or die("Can't open $filename for writing!");
        print $fh $error_msg;
        close($fh);
    }
}

sub configure {
    my ( $self, $args ) = @_;
    my $cgi = $self->{'cgi'};

    ## Grab the values we already have for our settings, if any exist
    my $framework_options = Koha::BiblioFrameworks->search({}, { order_by => ['frameworktext'] });
    my $normalize_utf8_normalization_form_options = ['D', 'C', 'KD', 'KC'];

    #TODO: make package global, our?
    my $default_incoming_record_items_tag = '949';
    my $default_normalize_utf8_normalization_form = 'C';

    unless ($cgi->param('save')) {
        my $template = $self->get_template({ file => 'configure.tt' });
        ## Grab the values we already have for our settings, if any exist
        $template->param(
            framework_options => $framework_options,
            framework => $self->retrieve_data('framework') // '',
            process_incoming_record_items_enable  => $self->retrieve_data('process_incoming_record_items_enable') || '0',
            incoming_record_items_tag  => $self->retrieve_data('incoming_record_items_tag') // $default_incoming_record_items_tag,
            normalize_utf8_enable => $self->retrieve_data('normalize_utf8_enable') || '0',
            normalize_utf8_normalization_form_options => $normalize_utf8_normalization_form_options,
            normalize_utf8_normalization_form => $self->retrieve_data('normalize_utf8_normalization_form') // $default_normalize_utf8_normalization_form,
            deduplicate_fields_tagspecs => $self->retrieve_data('deduplicate_fields_tagspecs') // '', # '035a'
            deduplicate_fields_enable => $self->retrieve_data('deduplicate_fields_enable') || '0',
            move_incoming_control_number_enable => $self->retrieve_data('move_incoming_control_number_enable') || '0',
            record_matching_enable => $self->retrieve_data('record_matching_enable') || '0',
            matchpoints => $self->retrieve_data('matchpoints') // '', # 'system-control-number,035a'
            stash_failed_records_enable => $self->retrieve_data('stash_failed_records_enable') || '0',
            stash_failed_records_directory => $self->retrieve_data('stash_failed_records_directory') // '',
            protect_authority_linkage_enable => $self->retrieve_data('protect_authority_linkage_enable') || '0'
        );
        print $cgi->header();
        print $template->output();
    }
    else {
        sub validate_option {
            my ($option, $valid_options, $label) = @_;
            if (!(any { $_ eq $option } @{$valid_options})) {
                die("Invalid option for \"$label\": \"$option\"");
            }
        }
        my $config = {
            framework => $cgi->param('framework') // '',
            process_incoming_record_items_enable  => $cgi->param('process_incoming_record_items_enable') || '0',
            incoming_record_items_tag  => $cgi->param('incoming_record_items_tag') // '',
            normalize_utf8_enable => $cgi->param('normalize_utf8_enable') || '0',
            normalize_utf8_normalization_form => $cgi->param('normalize_utf8_normalization_form') // $default_normalize_utf8_normalization_form,
            deduplicate_fields_enable => $cgi->param('deduplicate_fields_enable') || '0',
            deduplicate_fields_tagspecs => $cgi->param('deduplicate_fields_tagspecs') // '',
            move_incoming_control_number_enable => $cgi->param('move_incoming_control_number_enable') || '0',
            record_matching_enable => $cgi->param('record_matching_enable') || '0',
            matchpoints => $cgi->param('matchpoints') // '',
            stash_failed_records_enable => $cgi->param('stash_failed_records_enable') || '0',
            stash_failed_records_directory => $cgi->param('stash_failed_records_directory') // '',
            protect_authority_linkage_enable => $cgi->param('protect_authority_linkage_enable') || '0',
            last_configured_by => C4::Context->userenv->{'number'}
        };
        #TODO: regexp validation for non-options settings
        # Seems reset is not necessary?
        $framework_options->reset;
        my @valid_frameworkcodes = ();
        push @valid_frameworkcodes, '';
        while (my $row = $framework_options->next) {
            push @valid_frameworkcodes, $row->get_column('frameworkcode');
        }

        # Validate
        validate_option($config->{framework}, \@valid_frameworkcodes, 'Framework');
        my $checkbox_options = ['0', '1'];
        validate_option($config->{normalize_utf8_enable}, $checkbox_options, 'Normalize UTF-8');
        validate_option($config->{move_incoming_control_number_enable}, $checkbox_options, 'Move incoming control number');
        validate_option($config->{process_incoming_record_items_enable}, $checkbox_options, 'Process incoming items');
        validate_option($config->{deduplicate_fields_enable}, $checkbox_options, 'Enable deduplicate fields');
        validate_option($config->{stash_failed_records_enable}, $checkbox_options, 'Stash field records');
        validate_option($config->{protect_authority_linkage_enable}, $checkbox_options, 'Protect autority linkage');

        # Save
        $self->store_data($config);
        $self->go_home();
    }
}

## This is the 'install' method. Any database tables or other setup that should
## be done when the plugin if first installed should be executed in this method.
## The installation method should always return true if the installation succeeded
## or false if it failed.
#sub install() {
#    my ( $self, $args ) = @_;

#    my $table = $self->get_qualified_table_name('mytable');

#    return C4::Context->dbh->do( "
#        CREATE TABLE  $table (
#            `borrowernumber` INT( 11 ) NOT NULL
#        ) ENGINE = INNODB;
#    " );
#}

## This method will be run just before the plugin files are deleted
## when a plugin is uninstalled. It is good practice to clean up
## after ourselves!
#sub uninstall() {
#    my ( $self, $args ) = @_;

#    my $table = $self->get_qualified_table_name('mytable');

#    return C4::Context->dbh->do("DROP TABLE $table");
#}
#

## HELPER FUNCTIONS ###
sub marc_record_deleted {
    my ($record) = @_;
    return substr($record->leader(), 5, 1) eq 'd';
}

# Return fields in $fields_a and $fields_b with values present in both
# both arrays, result sets are returned in the order of fields in $fields_a
sub marc_record_fields_intersect {
    my ($fields_a, $fields_b, $subfield) = @_;
    my %fields_b_set;
    my %result_set;
    my @result_set_a;
    my @result_set_b;
    if ($subfield) {
        foreach my $field (@{$fields_b}) {
            $fields_b_set{$field->subfield($subfield)} = $field;
        }
    }
    else {
        foreach my $field (@{$fields_b}) {
            $fields_b_set{$field->data()} = $field;
        }
    }
    foreach my $field (@{$fields_a}) {
        my $data = $subfield ? $field->subfield($subfield): $field->data();
        if (!$result_set{$data} && exists $fields_b_set{$data}) {
            push @result_set_a, $field;
            push @result_set_b, $fields_b_set{$data};
        }
    }
    return (\@result_set_a , \@result_set_b);
}

sub marc_record_tag_spec_expand_tags {
    my ($record, $tag_spec) = @_;
    my %tags;
    if ($tag_spec =~ /\./) {
        foreach my $field ($record->field($tag_spec)) {
            $tags{$field->tag()} = undef;
        }
    }
    else {
        $tags{$tag_spec} = undef;
    }
    return keys %tags;
}

# @FIXME? IMPORTANT: This function will delete all fields where the dedup-subfield is unset,
# but only if another duplicate is found. This is perhaps not very well-behaved, and should
# be fixed
sub in_place_dedup_record_field {
    my($record, $tag, $subfield) = @_;
    my %deduplicate_fields_tagspecs;
    my $key;
    my @fields = $record->field($tag);

    my $has_dups = 0;
    foreach my $field (@fields) {
        $key = $subfield ? $field->subfield($subfield) : $field->data();
        if ($key) {
            if (exists $deduplicate_fields_tagspecs{$key}) {
                $has_dups = 1;
            }
            else {
                $deduplicate_fields_tagspecs{$key} = $field;
            }
        }
    }
    if ($has_dups) {
        $record->delete_fields(@fields);
        $record->insert_fields_ordered(values %deduplicate_fields_tagspecs);
    }
}

sub build_simplequery {
    my ($matchpoint, $record, $operator) = @_;
    if ($operator) {
        die("Invalid operator: $operator") unless ($operator eq 'OR' || $operator eq 'AND');
    }
    else {
        $operator = 'AND';
    }

    my @search_strings;
    my ($index_field, $record_tagspec) = split (/,/, $matchpoint);
    if (MARC::Field->is_controlfield_tag($record_tagspec)) {
        # THIS will crash and burn, must be wrapped in foreach?
        my $record_data = $record->field($record_tagspec)->data();
        if ($record_data) {
            push (@search_strings, "$index_field:\"$record_data\"");
        }
    }
    elsif ($record_tagspec =~ /(\d{3})(.*)/) {
        my ($tag, $subfields) = ($1, $2);
        foreach my $field ($record->field($tag)) {
            my $record_data = $field->as_string($subfields);
            if ($record_data) {
                push (@search_strings, "$index_field:\"$record_data\"");
            }
        }
    }
    else {
        # @TODO: or use exceptions?
        die("Invalid matchpoint format, invalid marc-field: $matchpoint\n");
    }
    my $QParser = C4::Context->queryparser if (C4::Context->preference('UseQueryParser'));
    my $using_elastic_search = (C4::Context->preference('SearchEngine') eq 'Elasticsearch');
    my $op;
    # This is not pretty:
    if ($QParser) {
        $op = $operator eq 'OR' ? '||' : '&&';
    }
    elsif ($using_elastic_search) {
        $op = $operator eq 'OR' ? 'OR' : 'AND';
    }
    else {
        $op = $operator eq 'OR' ? 'or' : 'and';
    }
    return join(" $op ", @search_strings);
}
1;
