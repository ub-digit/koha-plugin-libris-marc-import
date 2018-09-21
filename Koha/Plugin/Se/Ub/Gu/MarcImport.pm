package Koha::Plugin::Se::Ub::Gu::MarcImport;

use Modern::Perl;
use Encode qw(encode);
use IPC::Open3;
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
use Koha::Caches;

# use Koha::Logger;
use Log::Log4perl;
use Log::Log4perl::MDC;
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
use UUID;

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

my $cache = Koha::Caches->get_instance();

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
    $self->init_log4perl();

    return $self;
}

sub init_log4perl {
    my ($self) = @_;
    my $config = $self->get_config();
    if ($config->{log4perl_config_file}) {
        Log::Log4perl->init($config->{log4perl_config_file});
    }
}

sub get_logger {
    my ($self) = @_;
    my $config = $self->get_config();
    if ($config->{log4perl_config_file}) {
        return Log::Log4perl->get_logger('Koha::Plugin::Se::Ub::Gu::MarcImport');
    }
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
    my ($self, $args) = @_;

    my $debug = $args->{debug};
    #my $logger = Koha::Logger->get;

    my @marc_records = ();
    my $marc_record = undef;
    my $marc_type = C4::Context->preference('marcflavour');
    my $config = $self->get_config();

    # Decode all records and catch possible decoding errors
    my $marc_batch;
    my $fh;
    # Remove block?
    {
        open($fh, "<", \$args->{data});
        binmode $fh, ':raw';
        $marc_batch = MARC::Batch->new('USMARC', $fh);
    }

    my @records;
    my $i = -1;
    while(1) {
        $i++;
        my $record;
        # Infinite loop and eval so can catch marc decode/parsing errors
        eval { $record = $marc_batch->next() };
        my $errors = $@ || join("\n", $marc_batch->warnings());
        if ($errors) {
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
            $self->_importError(
                $record_data,
                $record,
                "invalid_record",
                "Failed to parse MARC record: $errors"
            );
            next;
        }
        elsif(!$record) {
            last;
        }
        push @records, $record;
    }
    $args->{data} = encode('UTF-8', join("", map { $_->as_usmarc() } @records), 1) || undef;
    return undef unless $args->{data};

    if ($config->{run_marc_command_enable} && $config->{run_marc_command_command}) {
        {
            my $uuid;
            my $uuid_string;
            UUID::generate($uuid);
            UUID::unparse($uuid, $uuid_string);

            # TODO: Base dir as config option?
            my $marc_filename = "MarcImport_run_marc_command_marc_file-$uuid_string";
            my $marc_file = "/tmp/$marc_filename";
            my $command = $config->{run_marc_command_command};

            $command =~ s/\{marc_filename\}/$marc_filename/g;
            my $replacements = $command =~ s/\{marc_file\}/$marc_file/g;
            if (!$replacements) {
                $self->_importError(
                    $args->{data},
                    undef,
                    "run_marc_command_missing_marc_file_token",
                    "Missing marc file token, command must contain {marc_file} which will be replaced with input marc file path"
                );
                return;
            }

            open(FILE, ">", $marc_file);
            print FILE $args->{data};
            close(FILE);

            local $/ = undef;
            my $pid = open3(\*WRITER, \*READER, \*ERROR, $command);
            my $marc = <READER>;
            my $error = <ERROR>;
            waitpid($pid, 0) or die("$!\n");
            if ($?) {
                $self->_importError(
                    $args->{data},
                    undef,
                    "run_marc_command_fail",
                    "Command returned non zero exit status: $error"
                );
                return;
            }
            $args->{data} = $marc;
        }
    }

    # Remove block?
    {
        open($fh, "<", \$args->{data});
        binmode $fh, ':raw';
        $marc_batch = MARC::Batch->new('USMARC', $fh);
    }
    @records = ();
    while(my $record = $marc_batch->next()) {
        push @records, $record;
    }

    my @processed_marc_records;

    my @valid_utf8_normalization_forms = ('D', 'C', 'KD', 'KC');
    #my %valid_item_types = %{ ItemTypes() };


    foreach my $key ('deduplicate_fields_tagspecs', 'deduplicate_records_tagspecs', 'matchpoints') {
        $config->{$key} = [split(/[\r\n]+/, $config->{$key})] if defined $config->{$key};
    }

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


    if ($config->{deduplicate_records_enable} && $config->{deduplicate_records_tagspecs}) {
        my $hashed_fields_key;
        my %deduplicated_records;
        my @deduplicated_records_keys; # To preserve order
        my @missing_fields_records; #TODO: Hmm??
        my %hash_fields;
        my @keys;

        foreach my $field_spec (@{$config->{deduplicate_records_tagspecs}}) {
            my ($tag, $subfields) = $field_spec =~ /([0-9]{3})([a-zA-Z0-9]*)/;
            $hash_fields{$tag} = $subfields ? { map { $_ => undef } split(//, $subfields) } : undef;
        }
        foreach my $record (@records) {
            @keys = ();
            foreach my $field (grep { exists $hash_fields{$_->tag()} } $record->fields()) {
                my $subfields = $hash_fields{$field->tag()};
                # TODO: Rename sub to just hash_field or similiar
                my $key = hash_field_by_subfields($field, $subfields);
                if ($key) {
                    push @keys, $key;
                }
            }
            # Join on field terminator and sort make sure order does not matter
            $hashed_fields_key = join("\x1E", sort @keys);
            if ($hashed_fields_key) {
                if (exists $deduplicated_records{$hashed_fields_key}) {
                    # Remove key
                    @deduplicated_records_keys = grep { $_ ne $hashed_fields_key } @deduplicated_records_keys;
                }
                $deduplicated_records{$hashed_fields_key} = $record;
                push @deduplicated_records_keys, $hashed_fields_key;
            }
            else {
                push @missing_fields_records, $record;
            }
        }
        @records = map { $deduplicated_records{$_} } @deduplicated_records_keys;
        # TODO: Right now these end up at the bottom
        # to preserve order could generege guid hash key
        # instead
        if (@missing_fields_records) {
            push @records, @missing_fields_records;
        }
    }

    RECORD: foreach my $record (@records) {
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

                        $self->_importError(
                            $record->as_usmarc(),
                            $record,
                            "multiple_matches",
                            $error
                        );
                        next RECORD;
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
                # TODO: $field_spec/$tagspecs, pick one!
                foreach my $field_spec (@{$config->{deduplicate_fields_tagspecs}}) {
                    my ($tag_spec, $subfields) = $field_spec =~ /([0-9.]{3})([a-zA-Z0-9]*)/;
                    if (!$tag_spec) {
                        die "Empty or invalid tag-spec: \"$tag_spec\"";
                    }
                    foreach my $tag (marc_record_tag_spec_expand_tags($record, $tag_spec)) {
                        in_place_dedup_record_field($record, $tag, $subfields ? [split //, $subfields] : undef);
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
    return encode('UTF-8', join("", map { $_->as_usmarc() } @processed_marc_records), 1) || undef;
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

sub _importError {
    my ($self, $record_data_string, $record, $error_type, $error_msg, $directory) = @_;
    my $config = $self->get_config();
    my $record_data = encode('UTF-8', $record_data_string);

    if ($config->{'stash_failed_records_enable'}) {
        $self->_stashFailedMarcRecord(
            $record_data,
            $error_type,
            $error_msg,
            $config->{'stash_failed_records_directory'}
        );
    }
    my $logger = $self->get_logger();
    if ($logger) {

        my $user_email = defined C4::Context::userenv && C4::Context::userenv->{'emailaddress'};
        Log::Log4perl::MDC->put('to', $user_email) if $user_email;

        Log::Log4perl::MDC->put('subject', "Koha marc import error: $error_type");
        Log::Log4perl::MDC->put('attachment-data', $record_data);
        Log::Log4perl::MDC->put('attachment-filename', 'records.mrc');
        my $message = "$error_msg\n\n";
        $message .= "Record:\n" . $record->as_formatted() if defined $record;
        $logger->error($message);
    }
}

sub _stashFailedMarcRecord {
    my ($self, $record_data, $error_type, $error_msg, $directory) = @_;
    if (length($directory)) {
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
    else {
        # warn?
    }
}

sub get_config_defaults {
    my $default_normalize_utf8_normalization_form = 'C';
    return {
        'framework' => '',
        'log4perl_config_file' => '',
        'process_incoming_record_items_enable' => '0',
        'incoming_record_items_tag' => '',
        'normalize_utf8_enable' => '0',
        'normalize_utf8_normalization_form' => $default_normalize_utf8_normalization_form,
        'deduplicate_fields_enable' => '0',
        'deduplicate_fields_tagspecs' => '',
        'deduplicate_records_enable' => '0',
        'deduplicate_records_tagspecs' => '',
        'move_incoming_control_number_enable' => '0',
        'record_matching_enable' => '0',
        'matchpoints' => '',
        'stash_failed_records_enable' => '0',
        'stash_failed_records_directory' => '',
        'protect_authority_linkage_enable' => '0',
        'run_marc_command_enable' => '0',
        'run_marc_command_command' => '',
    };
}

my $config_cache_key = 'Koha::Plugin::Se::Ub::Gu::MarcImport_config';
sub get_config {
    my ($self) = @_;

    my $config = $cache->get_from_cache($config_cache_key, { unsafe => 1 }) unless $self->{no_cache};
    if (!$config) {
        my $defaults = $self->get_config_defaults();
        $config = {};
        foreach my $key (keys %{$defaults}) {
            $config->{$key} = $self->retrieve_data($key) // $defaults->{$key};
        }
        $cache->set_in_cache($config_cache_key, $config) unless $self->{no_cache};
    }
    return $config;
}

sub store_data {
    my ($self, $data) = @_;
    $self->SUPER::store_data($data);
    $cache->clear_from_cache($config_cache_key);
}

sub configure {
    my ($self, $args) = @_;
    my $cgi = $self->{'cgi'};

    ## Grab the values we already have for our settings, if any exist
    my $framework_options = Koha::BiblioFrameworks->search({}, { order_by => ['frameworktext'] });
    my $normalize_utf8_normalization_form_options = ['D', 'C', 'KD', 'KC'];

    if ($cgi->param('save')) {
        sub validate_option {
            my ($option, $valid_options, $label) = @_;
            if (!(any { $_ eq $option } @{$valid_options})) {
                die("Invalid option for \"$label\": \"$option\"");
            }
        }
        my $config = {
            last_configured_by => C4::Context->userenv->{'number'}
        };
        my $defaults = $self->get_config_defaults();
        foreach my $key (keys %{$defaults}) {
            $config->{$key} = $cgi->param($key) // $defaults->{$key};
        }

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
        validate_option($config->{deduplicate_records_enable}, $checkbox_options, 'Enable deduplicate records');
        validate_option($config->{stash_failed_records_enable}, $checkbox_options, 'Stash field records');
        validate_option($config->{protect_authority_linkage_enable}, $checkbox_options, 'Protect autority linkage');
        validate_option($config->{run_marc_command_enable}, $checkbox_options, 'Process marc command');
        # TODO: why not validate here instead using @valid_utf8_normalization_forms?

        # Save
        $self->store_data($config);
        $self->go_home();
    }
    else {
        my $template = $self->get_template({ file => 'configure.tt' });
        ## Grab the values we already have for our settings, if any exist
        $template->param(
            framework_options => $framework_options,
            normalize_utf8_normalization_form_options => $normalize_utf8_normalization_form_options,
            %{$self->get_config}
        );
        print $cgi->header(-charset => 'utf-8');
        print $template->output();
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

sub hash_field_by_subfields {
    my ($field, $subfields_hash) = @_;
    my $hash_data;
    if ($field->is_control_field()) {
        $hash_data = $field->data();
    }
    else {
        if (defined $subfields_hash && ref($subfields_hash) eq 'ARRAY') {
            $subfields_hash = { map { $_ => undef } @{$subfields_hash} };
        }
        $hash_data = join("\x1E", (sort
                (map { join("\x1F", @{$_}) }
                    ($subfields_hash ? grep { exists $subfields_hash->{$_->[0]} } $field->subfields() : $field->subfields()))));
    }
    return $hash_data ? $field->tag() . ":" . $hash_data : undef;
}

sub in_place_dedup_record_field {
    my ($record, $tag, $subfields) = @_;
    my %deduplicated_fields;
    my @undef_subfields_fields;
    my $key;
    my @fields = $record->field($tag);
    my $subfields_hash = $subfields ? { map { $_ => undef } @{$subfields} } : undef;

    my $has_dups = 0;
    foreach my $field (@fields) {
        $key = hash_field_by_subfields($field, $subfields_hash);
        if (defined $key) {
            if (exists $deduplicated_fields{$key}) {
                # Don't add and set marker we need to modify record
                $has_dups = 1;
            }
            else {
                $deduplicated_fields{$key} = $field;
            }
        }
        else {
            # Keep all fields with undefined subfields
            push @undef_subfields_fields, $field;
        }
    }
    if ($has_dups) {
        $record->delete_fields(@fields);
        $record->insert_fields_ordered(values %deduplicated_fields);
        if (@undef_subfields_fields) {
            $record->insert_fields_ordered(@undef_subfields_fields);
        }
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
