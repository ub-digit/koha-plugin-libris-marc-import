package Koha::Plugin::Se::Ub::Gu::MarcImport;

use Modern::Perl;

## Required for all plugins
use base qw(Koha::Plugins::Base);

## We will also need to include any Koha libraries we want to access
use C4::Context;
use C4::Biblio;
use C4::Items;
use C4::Search;
use C4::Charset qw(SetUTF8Flag);
use Koha::SearchEngine;
use Koha::SearchEngine::Search;
use Koha::BiblioFrameworks;
use MARC::Record;
use List::MoreUtils qw(all any);
use Unicode::Normalize qw(normalize check);

## Here we set our plugin version
our $VERSION = 0.01;

## Here is our metadata, some keys are required, some are optional
our $metadata = {
    name   => 'GUB Marc Import Plugin',
    author => 'David Gustafsson',
    description => 'Custom marc import tweaks (@todo: proper desc)',
    date_authored   => '2017-02-07',
    date_updated    => '2017-02-21',
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

    my $marc_records = $args->{data};
    my @processed_marc_records;

    my @valid_utf8_normalization_forms = ('D', 'C', 'KD', 'KC');

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

    # @FIXME: This feels a litte bit shaky?
    my ($koha_items_tag) = GetMarcFromKohaField('items.itemnumber', $config->{framework});

    # Libris => Koha mapping
    # @TODO(!) this can not be hard coded, must be feted from koha items fields mappings
    # my $mss = GetMarcSubfieldStructure($frameworkcode);
    my %libris_koha_subfield_mappings = (
        'F' => 't', #t: items.copynumber
        'X' => 'y', #y: items.itype
        '6' => 'p', #p: items.barcode
        'a' => 'o', #o: items.itemcallnumber
        #'s' => ??
        #'t' => ??
        #'K' => ??,
    );
    # Properties for which if all equal two items considered equal
    # TODO: Could make this configurable?
    my @item_comparison_props = (
        #'location', # Redundant because of previous check
        'itype',
        'itemcallnumber',
        'copynumber',
        # FIXME?: This is perhaps a bit cryptic, but since 'itemnotes' (z) has been set
        # above, including it in comparison parameters will result in a check if existing item
        # has z set to '1', and if so, don't add this item, which is what we want
        'itemnotes',
        #'barcode', # Redundant because of previous check
    );

    RECORD: foreach my $record (@{$marc_records}) {
        my $koha_local_record_id = $record->subfield($koha_local_id_tag, $koha_local_id_subfield);
        if ($koha_local_record_id && !GetBiblio($koha_local_record_id)) {
            # @TODO: Probalby from other koha instance, or some other issue
            # What to do?
            $koha_local_record_id = undef;
        }
        # @TODO: move this?
        ## Move incoming control number
        if ($config->{move_incoming_control_number_enable}) {
            my $control_number_identifier_field = $record->field('003');
            my $system_control_number = (
                $control_number_identifier_field && $control_number_identifier_field->data ?
                '(' . $control_number_identifier_field->data . ')' : ''
            ) . $record->field('001')->data;

            my @record_system_control_numbers = $record->subfield('035', 'a');
            # Add if not already present
            if(!(@record_system_control_numbers && (any { $_ eq $system_control_number } @record_system_control_numbers))) {
                $record->insert_fields_ordered(MARC::Field->new('035', ' ', ' ', 'a' => $system_control_number));
            }
        }
        my $matched_record_id = $koha_local_record_id;

        #my $sequential_match = [
        #   'system-control-number,035a',
        #   'local-number,999c',
        #];

        ## Perform search engine based record matching
        if (!$matched_record_id && $config->{record_matching_enable}) {
            SEQUENTIAL_MATCH: foreach my $matchpoint (@{$config->{matchpoints}}) {
                my $query = build_simplequery($matchpoint, $record, 'OR');
                if ($query) {
                    my ($error, $results, $totalhits) = $searcher->simple_search_compat($query, 0, 3, [$server]);
                    if (defined $error) {
                        # TODO: improve error handling
                        warn "Search error: $error";
                        # TODO: or die?
                        next SEQUENTIAL_MATCH;
                    }
                    $results //= [];

                    if (@{$results} == 1) {
                        my $_record = C4::Search::new_record_from_zebra($server, $results->[0]);
                        SetUTF8Flag($_record); # From bulkmarcimport, can remove this?
                        $matched_record_id = $_record->subfield($koha_local_id_tag, $koha_local_id_subfield);
                        last SEQUENTIAL_MATCH;
                    }
                    elsif (@{$results} > 1) {
                        warn "Nore than one match for $query";
                    }
                    else {
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
                    warn "ERROR: Delete biblio $matched_record_id failed: $error\n";
                    # @TODO: Or die?
                    next RECORD;
                }
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
            # Dedupe records, and match up fields
            # 1. Dedup incoming record field
            # 2. Dedup existing
            # 3. Intersect the two fields
            # 4. Remove intersection from both records
            # 5. Put intersection at front for both records, sorted by incoming order

            ## Dedup incoming record field
            my $matched_record = $matched_record_id ? GetMarcBiblio($matched_record_id) : undef;
            if ($config->{deduplicate_fields_enable}) {
                foreach my $field_spec (@{$config->{deduplicate_fields_tagspecs}}) {
                    my ($tag_spec, $subfield) = $field_spec =~ /([0-9.]{3})([a-z]?)/;
                    if (!$tag_spec) {
                        die "Empty or invalid tag-spec: \"$tag_spec\"";
                    }
                    foreach my $tag (marc_record_tag_spec_expand_tags($record, $tag_spec)) {
                        in_place_dedup_record_field($record, $tag, $subfield);
                        if ($matched_record) {
                            # The reason why we return fields of both records is that
                            # they are still not guarantied to be unique.
                            # For example, the specified subfield might match, but other
                            # subfields might differ
                            # This is a bit of a kludge, the alternative would be to delete the
                            # current record's field for example (and replace with incoming)
                            # Both returned sets are in the order of fields in first argument

                            #my @record_field = $record->field($tag);
                            #my @matched_record_field = $matched_record->field($tag);

                            my ($intersect_record_fields, $intersect_matched_record_fields) = marc_record_fields_intersect(
                                #\@record_field,
                                #\@matched_record_field,
                                [$record->field($tag)],
                                [$matched_record->field($tag)],
                                $subfield
                            );

                            if (@{$intersect_record_fields}) {
                                # Delete existing record fields in intersection
                                $matched_record->delete_fields(@{$intersect_matched_record_fields});

                                # Re-add fields of existing record at the front, in order of intersected fields
                                # of incoming record
                                my $before_field = $matched_record->field($tag);
                                if ($before_field) {
                                    $matched_record->insert_fields_before($before_field, @{$intersect_matched_record_fields});
                                }
                                else {
                                    $matched_record->insert_fields_ordered(@{$intersect_matched_record_fields});
                                }

                                # Do the same thing with incoming record
                                $record->delete_fields(@{$intersect_record_fields});
                                $before_field = $record->field($tag);
                                if ($before_field) {
                                    $record->insert_fields_before($before_field, @{$intersect_record_fields});
                                }
                                else {
                                    $record->insert_fields_ordered(@{$intersect_record_fields});
                                }
                            }
                        }
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

            if ($options->{process_incoming_record_items_enable}) {
                my @koha_item_fields = ();
                foreach my $libris_item_field ($record->field($config->{incoming_record_items_tag})) {
                    my %subfield_values = ();
                    my $data;
                    foreach my $libris_subfield (keys %libris_koha_subfield_mappings) {
                        $data = $libris_item_field->subfield($libris_subfield);
                        if ($data) {
                            $subfield_values{ $libris_koha_subfield_mappings{ $libris_subfield } } = $data;
                        }
                    }
                    # Special cases:
                    $data = $libris_item_field->subfield('D');
                    if ($data) {
                        $subfield_values{'c'} = substr $data, 3; #c: items.location
                    }
                    if (%subfield_values) {
                        $subfield_values{'z'} = '1';
                        # @TODO: get items field from koha instead
                        push @koha_item_fields, MARC::Field->new($koha_items_tag, '', '', %subfield_values);
                    }
                }
                # Only need to bother if there is any actual item incoming items
                if (@koha_item_fields) {
                    my @new_koha_item_fields = ();
                    my @existing_koha_items = ();
                    if ($matched_record_id) {
                        my $existing_koha_itemnumbers = GetItemnumbersForBiblio($matched_record_id);
                        foreach my $itemnumber (@{$existing_koha_itemnumbers}) {
                            push @existing_koha_items, GetItem($itemnumber);
                        }
                    }
                    ITEMFIELD: foreach my $item_field (@koha_item_fields) {
                        my $incoming_item = GetKohaItemsFromMarcField($item_field, $config->{framework});
                        # First check for possibly existing barcodes (globally)
                        if (GetItemnumberFromBarcode($incoming_item->{'barcode'})) {
                            next ITEMFIELD;
                        }
                        foreach my $existing_item (@existing_koha_items) {
                            # If has the same shelving location code the two items are concidered equal
                            if (
                                $incoming_item->{'location'} eq $existing_item->{'location'} ||
                                all { $existing_item->{$_} eq $incoming_item->{$_} } @item_comparison_props
                            ) {
                                next ITEMFIELD;
                            }
                        }
                        push @new_koha_item_fields, $item_field;
                    }
                    if (@new_koha_item_fields) {
                        $record->insert_fields_ordered(@new_koha_item_fields);
                    }
                }
            }
            push @processed_marc_records, $record;
        }
    }
    return \@processed_marc_records;
}

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
            last_configured_by => C4::Context->userenv->{'number'},
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

sub in_place_dedup_record_field {
    my($record, $tag, $subfield) = @_;
    my %deduplicate_fields_tagspecs;
    my $key;
    my @fields = $record->field($tag);
    my $has_dups = 0;
    foreach my $field (@fields) {
        $key = $subfield ? $field->subfield($subfield) : $field->data();
        if (exists $deduplicate_fields_tagspecs{$key}) {
            $has_dups = 1;
        }
        else {
            $deduplicate_fields_tagspecs{$key} = $field;
        }
    }
    if ($has_dups) {
        #print "HAS DUPS!\n";
        $record->delete_fields(@fields);
        $record->insert_fields_ordered(values %deduplicate_fields_tagspecs);
    }
    else {
        #print "HAS NO DUPS!\n";
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
    #TODO: bug in marcbulkimport, unable to match on non subfields?
    #
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
        warn "Invalid matchpoint format, invalid marc-field: $matchpoint\n";
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
