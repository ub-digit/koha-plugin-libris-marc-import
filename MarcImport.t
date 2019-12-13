#!/usr/bin/env perl

use Modern::Perl;

use MARC::Field;
use MARC::Record;
use MARC::File::USMARC;
use Encode qw(encode);
use File::Spec;

#use Test::MockModule;
use Test::MockObject::Extends;
use t::lib::Mocks;
use Test::More tests => 55;
use Cwd qw(getcwd);

use C4::Biblio;
use C4::Items;
use C4::Context;

use Koha::ItemType;
use Koha::Library;
use Koha::AuthorisedValue;
use Koha::Plugins;
use Koha::Database;

use File::Spec::Functions; # catfile
use File::Basename;

#my $plugin_path = File::Spec->catfile(getcwd, 'Koha', 'Plugin', 'Se', 'Ub', 'Gu');
use lib 'Koha/Plugin/Se/Ub/Gu'; # Perhaps not needed after all?? Koha loads include path?
use Koha::Plugin::Se::Ub::Gu::MarcImport;

C4::Context->_new_userenv('');
C4::Context->set_userenv(0, 0, 0, 'test', 'test', 0, 'test', undef, 'test@example.com', undef, undef);

my $plugin = Koha::Plugin::Se::Ub::Gu::MarcImport->new({ no_cache => 1 });
$plugin = Test::MockObject::Extends->new($plugin);

my $conf = {
    framework => '',
    process_incoming_record_items_enable  => 1,
    incoming_record_items_tag  => '949',
    normalize_utf8_enable => 1,
    normalize_utf8_normalization_form => 'D', #TODO: Test for this??
    deduplicate_fields_tagspecs => "035a\n949D\n9496",
    deduplicate_fields_enable => 1,
    deduplicate_records_tagspecs => "001\n003\n035a8",
    deduplicate_records_enable => 1,
    move_incoming_control_number_enable => 1,
    record_matching_enable => 1,
    matchpoints => 'system-control-number,035a',
    protect_authority_linkage_enable => 1,
};

$plugin->mock('retrieve_data', sub {
        my ($self, $key) = @_;
        return $conf->{$key};
    }
);

my $frameworkcode = ''; # TODO: Get this from pref?
my ($item_tag, $item_subfield) = C4::Biblio::GetMarcFromKohaField('items.itemnumber', $frameworkcode);
my $libris_item_tag = $plugin->retrieve_data('incoming_record_items_tag');
my ($koha_local_id_tag, $koha_local_id_subfield) = GetMarcFromKohaField('biblio.biblionumber', $frameworkcode);
my ($koha_items_tag) = GetMarcFromKohaField('items.itemnumber', $frameworkcode);

my $plugin_process_records = sub {
    my ($records, $test_message, $debug) = @_;
    $records = [$records] if (ref($records) ne 'ARRAY');
    my $processed_records_marc = $plugin->to_marc({
        data => join('', map { encode('UTF-8', $_->as_usmarc()) } @{$records} ),
        debug => $debug
    });
    ok($processed_records_marc, $test_message);
    # TODO: Quick and dirty, since MARCH::Batch interface is frustrating to work with
    return map { MARC::Record->new_from_usmarc($_) } (grep { $_ } split("\x1D", $processed_records_marc));
};

my $schema = Koha::Database->new->schema;
$schema->storage->txn_begin;

# Add item type
my $itemtype_code = 'test';
my $itemtype = Koha::ItemType->new({
		itemtype => $itemtype_code,
		description => $itemtype_code,
		rentalcharge => 0.0,
		defaultreplacecost => undef,
		processfee => undef,
		notforloan => 0,
		imageurl => '',
		summary => '',
		checkinmsg => '',
		checkinmsgtype => 'message',
		sip_media_type => '',
		hideinopac => 0,
		searchcategory => '',
	}
);

# TODO: Test for branch verification inside libris plugin??
# Add branch
eval { $itemtype->store; };
is($@, '', "Test item type was successfully saved");

my $library_branchcode = '12';
my $library = Koha::Library->new({
	branchcode => $library_branchcode,
	branchname => 'Test Library',
});

eval { $library->store; };
is($@, '', "Test library was successfully saved");

my $location = "${library_branchcode}3456";
my $location_authorized_value = Koha::AuthorisedValue->new({
		category => 'LOC',
		authorised_value => $location,
        #description => 'Test location', #TODO: Find out column name
		lib => $library_branchcode,
		lib_opac => undef,
		imageurl => '',
	}
);
eval { $location_authorized_value->store; };
is($@, '', "Test location was successfully saved");

my $record = MARC::Record->new;
$record->leader('03174nam a2200445 a 4500');
my @fields = (
    MARC::Field->new(
        '001',
        '1234'
    ),
    MARC::Field->new(
        '003',
        '003TEST'
    ),
    MARC::Field->new(
        '008',
        '970115m19979999maua          001 0 eng'
    ),
    MARC::Field->new(
        '035', ' ', ' ',
        a => '(035TEST)123',
    ),
    # Duplicate system-control-number
    MARC::Field->new(
        '035', ' ', ' ',
        a => '(035TEST)123',
    ),
    MARC::Field->new(
        100, '1', ' ',
        a => 'Knuth, Donald Ervin',
        d => '1938',
    ),
    MARC::Field->new(
        245, '1', '4',
        a => 'The art of computer programming',
        c => 'Donald E. Knuth.',
    ),
    MARC::Field->new(
        650, ' ', '0',
        a => 'Computer programming.',
        9 => '462',
    ),
);
$record->append_fields(@fields);

my ($processed_record) = $plugin_process_records->($record, "New incoming record was successfully processed by plugin");
my $matched_biblionumber = $processed_record->subfield($koha_local_id_tag, $koha_local_id_subfield);
is($matched_biblionumber, undef, "New incoming record processed by plugin does not match any existing Koha record");

@fields = $processed_record->field('035');
is(@fields, 2, "There is two 035 fields in total");
is(scalar( grep { $_->subfield('a') eq '(035TEST)123' } @fields ), 1, "Duplicate 035 field have been removed based on 035a match");
# TODO: also check of deleted from 001, 003, do we even delete??
is(scalar( grep { $_->subfield('a') eq '(003TEST)1234' } @fields ), 1, "Incoming control number (001 + 003) have been moved to 035a");

# Save processed record
my ($biblionumber) = AddBiblio($processed_record, $frameworkcode);
ok($biblionumber, "New incoming record processed by plugin was successfully saved");

# Hack: sleep 1 second so elasticsearch has time to index, can this be solved in a better way
# Slightly better way would be a retry loop to make this a bit more rebust
sleep(1);

# Now lets add some item data to incoming record while keeping existing fields the same
# TODO: (Perhaps also test the incoming items deduplication thingy!)

# TODO: Code duplication, instead import this from plugin class:
use constant {
    LIBRIS_ITEM_COPYNUMBER => 'F',
    LIBRIS_ITEM_TYPE => 'X',
    LIBRIS_ITEM_BARCODE => '6',
    LIBRIS_ITEM_CALLNUMBER => 'a',
    LIBRIS_ITEM_NOT_FOR_LOAN => 's',
    LIBRIS_ITEM_LOCATION => 'D',
    LIBRIS_ITEM_NOTE => 'z',
};

@fields = (
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, '1',
        LIBRIS_ITEM_TYPE, 'test',
        LIBRIS_ITEM_BARCODE , '123',
        LIBRIS_ITEM_CALLNUMBER, 'test callnumber',
        LIBRIS_ITEM_NOT_FOR_LOAN, '0',
        LIBRIS_ITEM_LOCATION, "000$location",
        LIBRIS_ITEM_NOTE, '', # Perhaps omit this since should not be set at all
    ),
);
my $incoming_record_with_item = $record->clone();
$incoming_record_with_item->append_fields(@fields);

# Process record again, this time we should have a matching record in Koha
($processed_record) = $plugin_process_records->($incoming_record_with_item, "Incoming record processed by plugin was successfully processed");
$matched_biblionumber = $processed_record->subfield($koha_local_id_tag, $koha_local_id_subfield);
ok($matched_biblionumber, "Incoming record processed by plugin has a matching Koha record");

my @libris_item_fields = $processed_record->field($libris_item_tag); # TODO: item/items
is(@libris_item_fields, 1, "Incoming record processed by plugin has one libris item");
is($libris_item_fields[0]->subfield(LIBRIS_ITEM_NOTE), '1', "Libris item has item note (subfield z) set to '1'");

my @koha_item_fields = $processed_record->field($koha_items_tag);
is(@koha_item_fields, 1, "Incoming record processed by plugin has one koha item");
is($koha_item_fields[0]->subfield('t'), '1', "Koha item field copynumber has the expected value");
is($koha_item_fields[0]->subfield('y'), 'test', "Koha item field type has the expected value");
is($koha_item_fields[0]->subfield('p'), '123', "Koha item field barcode has the expected value");
is($koha_item_fields[0]->subfield('o'), 'test callnumber', "Koha item field callnumber has the expected value");
is($koha_item_fields[0]->subfield('7'), '0', "Koha item field \"not for loan\" has the expected value");
is($koha_item_fields[0]->subfield('c'), $location, "Koha item field location has the expected value");
is($koha_item_fields[0]->subfield('a'), $library_branchcode, "Koha item field homebranch has the expected value");
is($koha_item_fields[0]->subfield('b'), $library_branchcode, "Koha item field holdingbranch has the expected value");

# Tests for item matching rules:
# TODO: This, actually does not save items, keep or remove?
# We clone here since ModBiblio strips item fields
my ($success) = ModBiblio($processed_record->clone(), $matched_biblionumber, $frameworkcode);
ok($success, "Matched Koha biblio was successfully updated with incoming record with one item processed by plugin");

my $itemnumber = (AddItemFromMarc($processed_record, $matched_biblionumber))[2];
ok($itemnumber, "Koha item in incoming record processed by plugin was successfully added to matched Koha biblio");

my $updated_matched_record = GetMarcBiblio({ biblionumber => $matched_biblionumber, embed_items => 1});
@koha_item_fields = $updated_matched_record->field($koha_items_tag);
is(@koha_item_fields, 1, "Updated matched record loaded with GetMarcBiblio() has one koha item.");

# TODO: Perhaps $updated_matched_record can be passed directly?
my $temp_item_marc = MARC::Record->new();
$temp_item_marc->append_fields(@koha_item_fields);
my $koha_item = TransformMarcToKoha($temp_item_marc, $frameworkcode, 'items');
ok($koha_item, "Item marc in loaded record successfully transformed to Koha item");
is($koha_item->{copynumber}, '1', "Koha item copynumber has the expected value");
is($koha_item->{itype}, 'test', "Koha item type has the expected value");
is($koha_item->{barcode}, '123', "Koha item barcode has the expected value");
is($koha_item->{itemcallnumber}, 'test callnumber', "Koha item callnumber has the expected value");
# is($koha_item->{}, '0', "Koha item \"not for loan\" has the expected value"); #TODO: no notforloan field, how is this handled in Koha perhaps must have value ???
is($koha_item->{location}, $location, "Koha item location has the expected value");
is($koha_item->{homebranch}, $library_branchcode, "Koha item homebranch has the expected value");
is($koha_item->{holdingbranch}, $library_branchcode, "Koha item holdingbranch has the expected value");

my $incoming_record_with_item_with_existing_barcode = $record->clone();
@fields = (
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, 'X',
        LIBRIS_ITEM_TYPE, 'X',
        LIBRIS_ITEM_BARCODE , '123',
        LIBRIS_ITEM_CALLNUMBER, 'X',
        LIBRIS_ITEM_NOT_FOR_LOAN, 'X',
        LIBRIS_ITEM_LOCATION, 'X',
        LIBRIS_ITEM_NOTE, '', # Perhaps omit this since should not be set at all
    ),
);
$incoming_record_with_item_with_existing_barcode->append_fields(@fields);
# Process record again, item should be discarded (since barcode exists)
# TODO: Should have helper sub for this
($processed_record) = $plugin_process_records->(
    $incoming_record_with_item_with_existing_barcode,
    "Incoming record with item with existing barcode was successfully processed"
);
is(
    $processed_record->field($koha_items_tag),
    undef,
    "Incoming record containing an item with same barcode as existing Koha item has no items after processed by plugin"
);

my $incoming_record_with_item_with_existing_location = $record->clone();
@fields = (
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, 'X',
        LIBRIS_ITEM_TYPE, 'X',
        LIBRIS_ITEM_BARCODE , 'X',
        LIBRIS_ITEM_CALLNUMBER, 'X',
        LIBRIS_ITEM_NOT_FOR_LOAN, 'X',
        LIBRIS_ITEM_LOCATION, "000$location",
        LIBRIS_ITEM_NOTE, '', # Perhaps omit this since should not be set at all
    ),
);
$incoming_record_with_item_with_existing_location->append_fields(@fields);
# Process record again, item should be discarded
# (since an existing koha item on matching record with same location exists)
($processed_record) = $plugin_process_records->(
    $incoming_record_with_item_with_existing_location,
    "Incoming record with item with existing location was successfully processed"
);


is($processed_record->field($koha_items_tag), undef, "Incoming record containing an item with same location as existing Koha item on matching record has no items after processed by plugin");

($success) = DelItem({ itemnumber => $itemnumber, biblionumber => $matched_biblionumber });
ok($success, "Item on matched Koha biblio successfully deleted");

# Test if item was properly deleted by processing record with item with same barcode and location as before, this time it should be recognized as a new item
my $incoming_record_with_item_with_existing_location_and_barcode = $record->clone();
@fields = (
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, 'X',
        LIBRIS_ITEM_TYPE, 'X',
        LIBRIS_ITEM_BARCODE , '123',
        LIBRIS_ITEM_CALLNUMBER, 'X',
        LIBRIS_ITEM_NOT_FOR_LOAN, 'X',
        LIBRIS_ITEM_LOCATION, "000$location",
        LIBRIS_ITEM_NOTE, '', # Perhaps omit this since should not be set at all
    ),
);
$incoming_record_with_item_with_existing_location_and_barcode->append_fields(@fields);
($processed_record) = $plugin_process_records->(
    $incoming_record_with_item_with_existing_location_and_barcode,
    "Incoming record with item with same barcode and locaiton as item that was deleted successfully processed by plugin"
);
@koha_item_fields = $processed_record->field($koha_items_tag);
is(
    @koha_item_fields,
    1,
    "Incoming record with item with same barcode and location as item that was deleted will create new Koha item"
);

# Process record again, item should be discarded (since a libris item with identical
# properties exists in matched Koha record).
# Note that we have deleted the Koha item to make sure that subfield mathing with
# matching libris item is the only way item could be discarded.
($processed_record) = $plugin_process_records->(
    $incoming_record_with_item,
    "Incoming record containing an item with matching properties to existing Koha record libris item field was successfully processed"
);
is(
    $processed_record->field($koha_items_tag),
    undef,
    "Incoming record containing an item with matching properties to existing Koha record libris item field has no items after processed by plugin"
);

# Append two more unique records just to make sure that multiple items are processed correctly
# Existing item:
# MARC::Field->new(
#   $libris_item_tag, ' ', ' ',
#   LIBRIS_ITEM_COPYNUMBER, '1',
#   LIBRIS_ITEM_TYPE, 'test',
#   LIBRIS_ITEM_BARCODE , '123',
#   LIBRIS_ITEM_CALLNUMBER, 'test callnumber',
#   LIBRIS_ITEM_NOT_FOR_LOAN, '0',
#   LIBRIS_ITEM_LOCATION, "000$location",
#   LIBRIS_ITEM_NOTE, '', # Perhaps omit this since should not be set at all
#);

my $incoming_record_with_duplicate_items = $incoming_record_with_item->clone();
@fields = (
    # New uniqe item
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, '1',
        LIBRIS_ITEM_TYPE, 'test',
        LIBRIS_ITEM_BARCODE , '1234',
        LIBRIS_ITEM_CALLNUMBER, 'test callnumber 2',
        LIBRIS_ITEM_NOT_FOR_LOAN, '1',
        LIBRIS_ITEM_LOCATION, "${library_branchcode}4567",
    ),
    # Add two items with same barcode to test deduplication
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, '1',
        LIBRIS_ITEM_TYPE, 'test',
        LIBRIS_ITEM_BARCODE , '12345',
        LIBRIS_ITEM_CALLNUMBER, 'test callnumber 3',
        LIBRIS_ITEM_NOT_FOR_LOAN, '1',
        LIBRIS_ITEM_LOCATION, "${library_branchcode}5678",
    ),
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, '1',
        LIBRIS_ITEM_TYPE, 'test',
        LIBRIS_ITEM_BARCODE , '12345',
        LIBRIS_ITEM_CALLNUMBER, 'test callnumber 4',
        LIBRIS_ITEM_NOT_FOR_LOAN, '1',
        LIBRIS_ITEM_LOCATION, "${library_branchcode}6789",
    ),
    # Add two items with same location to test deduplication
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, '1',
        LIBRIS_ITEM_TYPE, 'test',
        LIBRIS_ITEM_BARCODE , '123456',
        LIBRIS_ITEM_CALLNUMBER, 'test callnumber 5',
        LIBRIS_ITEM_NOT_FOR_LOAN, '1',
        LIBRIS_ITEM_LOCATION, "${library_branchcode}7890",
    ),
    MARC::Field->new(
        $libris_item_tag, ' ', ' ',
        LIBRIS_ITEM_COPYNUMBER, '1',
        LIBRIS_ITEM_TYPE, 'test',
        LIBRIS_ITEM_BARCODE , '1234567',
        LIBRIS_ITEM_CALLNUMBER, 'test callnumber 6',
        LIBRIS_ITEM_NOT_FOR_LOAN, '1',
        LIBRIS_ITEM_LOCATION, "${library_branchcode}7890",
    ),
);
$incoming_record_with_duplicate_items->append_fields(@fields);
($processed_record) = $plugin_process_records->(
    $incoming_record_with_duplicate_items,
    "Incoming record with duplicate items successfully processed by plugin"
);
@koha_item_fields = $processed_record->field($koha_items_tag);
is(
    @koha_item_fields,
    3,
    "The correct number of koha items was created for incoming libris items"
);
is(
    scalar( grep { $_->subfield('p') eq '1234' } @koha_item_fields ),
    1,
    "One koha item created for new unique incoming libris item"
);
is(
    scalar( grep { $_->subfield('p') eq '12345' } @koha_item_fields ),
    1,
    "Incoming items with identical barcodes has been deduplicated"
);
is(
    scalar( grep { $_->subfield('c') eq "${library_branchcode}7890" } @koha_item_fields ),
    1,
    "Incoming items with identical locations has been deduplicated"
);

my $record_a = $record->clone();
my $record_b = $record->clone();

# Set subfield "8" to make record_a unique
foreach my $field ($record_a->field('035')) {
    $field->add_subfields( '8' => '123' );
}
my @processed_records = $plugin_process_records->(
    [$record_a, $record_b],
    "Non-duplicate records successfully processed by plugin"
);

is(@processed_records, 2, "No record has been deduplicated");

# Set subfield "8" to same value for record_b,
# record_a should now be candidate for deduplication
foreach my $field ($record_b->field('035')) {
    $field->add_subfields( '8' => '123' );
}
@processed_records = $plugin_process_records->(
    [$record_a, $record_b],
    "Duplicate records successfully processed by plugin"
);
is(@processed_records, 1, "One record has been deduplicated");

$conf->{process_incoming_record_items_enable} = 0;
$conf->{normalize_utf8_enable} = 0;
$conf->{deduplicate_fields_enable} = 0;
$conf->{deduplicate_records_enable} = 0;
$conf->{move_incoming_control_number_enable} = 0;
$conf->{record_matching_enable} = 0;
$conf->{protect_authority_linkage_enable} = 0;
$conf->{run_marc_command_enable} = 1;
$conf->{run_marc_command_command} = 'touch /tmp/MarcImport_test && cat "{marc_file}" && rm "{marc_file}"';

my $run_marc_command_record = $plugin->to_marc({
    data => encode('UTF-8', $record_a->as_usmarc())
});

ok(-f '/tmp/MarcImport_test', "Process MARC command has run");
unlink '/tmp/MarcImport_test';

ok($run_marc_command_record, "Record processed by shell command successfully processed by plugin");
ok(encode('UTF-8', $record_a->as_usmarc()) eq $run_marc_command_record, 'Record produced by shell command `cat {marc_file}` is identical to original incoming record');

# TODO: Should also check that record_a was removed, and not record_b!

# Create duplicate record in database to trigger multiple matches error
my ($duplicate_record_biblionumber) = AddBiblio($processed_record, $frameworkcode);

# Sleep hack agian to wait for Elastic:
sleep(1);

# Enable matching again
$conf->{record_matching_enable} = 1;

$conf->{log4perl_config_file} = catfile(dirname(__FILE__), 'log4perl.t.conf');
$plugin->init_log4perl();

# TODO: Mock MimeMailSender and write tests for correct headers etc
my $result = $plugin->to_marc({
    data => encode('UTF-8', $record->as_usmarc())
});

is($result, undef, "Plugin method 'to_marc' should return undef on error");

# Need to clean up Zebra/Elastic explicitly since not part of transaction
ModZebra($_, "recordDelete", "biblioserver") foreach ($biblionumber, $duplicate_record_biblionumber);
$schema->storage->txn_rollback;
