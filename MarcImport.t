#!/usr/bin/env perl

use Modern::Perl;

use MARC::Field;
use MARC::Record;
use MARC::File::USMARC;
use C4::Biblio;
use C4::Items;

use Koha::ItemType;
use Koha::Library;
#use Koha::AuthorisedValues;
use Koha::AuthorisedValue;

#use Test::MockModule;
use Test::MockObject::Extends;
use t::lib::Mocks;
use Test::More tests => 36;
use C4::Context;
use Koha::Plugins;
use Koha::Database;
use Cwd qw(getcwd);
use File::Spec;
#my $plugin_path = File::Spec->catfile(getcwd, 'Koha', 'Plugin', 'Se', 'Ub', 'Gu');
use lib 'Koha/Plugin/Se/Ub/Gu'; # Perhaps not needed after all?? Koha loads include path?
use Koha::Plugin::Se::Ub::Gu::MarcImport;

my $plugin = Koha::Plugin::Se::Ub::Gu::MarcImport->new({});
$plugin = Test::MockObject::Extends->new($plugin);
$plugin->mock('retrieve_data', sub {
        my ($self, $key) = @_;
        my $conf = {
            framework => '',
            process_incoming_record_items_enable  => 1,
            incoming_record_items_tag  => '949',
            normalize_utf8_enable => 1,
            normalize_utf8_normalization_form => 'D', #TODO: Test for this??
            deduplicate_fields_tagspecs => "035a\n949D\n9496",
            deduplicate_fields_enable => 1,
            move_incoming_control_number_enable => 1,
            record_matching_enable => 1,
            matchpoints => 'system-control-number,035a',
            protect_authority_linkage_enable => 1,
        };
        return $conf->{$key};
    }
);

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

# TODO: Assert that created ($@ is empty)
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
    # System-contol-number
    MARC::Field->new(
        '035', ' ', ' ',
        a => '(035TEST)123',
    ),
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

# Add biblio without item
my $frameworkcode = ''; # TODO: Get this from pref?
my ($item_tag, $item_subfield) = C4::Biblio::GetMarcFromKohaField('items.itemnumber', $frameworkcode);
my $libris_item_tag = $plugin->retrieve_data('incoming_record_items_tag');
my ($koha_local_id_tag, $koha_local_id_subfield) = GetMarcFromKohaField('biblio.biblionumber', $frameworkcode);
my ($koha_items_tag) = GetMarcFromKohaField('items.itemnumber', $frameworkcode);

my $processed_record_marc = $plugin->to_marc({ data => $record->as_usmarc() });
my $processed_record = MARC::Record->new_from_usmarc($processed_record_marc);

my $matched_biblionumber = $processed_record->subfield($koha_local_id_tag, $koha_local_id_subfield);
is($matched_biblionumber, undef, "New incoming record processed by plugin does not match any existing Koha record");

@fields = $processed_record->field('035');
is(@fields, 2, "There should be two 035 fields in total");
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
$processed_record_marc = $plugin->to_marc({ data => $incoming_record_with_item->as_usmarc() });
# TODO: Add same test above
ok($processed_record_marc, "Incoming record processed by plugin was successfully processed");

$processed_record = MARC::Record->new_from_usmarc($processed_record_marc);
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
$processed_record_marc = $plugin->to_marc({
        data => $incoming_record_with_item_with_existing_barcode->as_usmarc()
    }
);
$processed_record = MARC::Record->new_from_usmarc($processed_record_marc);
is($processed_record->field($koha_items_tag), undef, "Incoming record containing an item with same barcode as existing Koha item has no items after processed by plugin");

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
$processed_record_marc = $plugin->to_marc({
        data => $incoming_record_with_item_with_existing_location->as_usmarc()
    }
);
$processed_record = MARC::Record->new_from_usmarc($processed_record_marc);
is($processed_record->field($koha_items_tag), undef, "Incoming record containing an item with same location as existing Koha item on matching record has no items after processed by plugin");

($success) = DelItem({ itemnumber => $itemnumber, biblionumber => $matched_biblionumber });
ok($success, "Item on matched Koha biblio successfully deleted");

# TODO: test again that no item with same barcode can be found!! Just to make sure has been deleted

# Process record again, item should be discarded (since a libris item with identical
# properties exists in matched Koha record).
# Note that we have deleted the Koha item to make sure that subfield mathing with
# matching libris item is the only way item could be discarded.
$processed_record_marc = $plugin->to_marc({
        data => $incoming_record_with_item->as_usmarc()
    }
);
$processed_record = MARC::Record->new_from_usmarc($processed_record_marc);
is($processed_record->field($koha_items_tag), undef, "Incoming record containing an item with matching properties to existing Koha record libris item field has no items after processed by plugin");

# Need to clean up Zebra/Elastic explicitly since not part of transaction
ModZebra($biblionumber, "recordDelete", "biblioserver");
$schema->storage->txn_rollback;
