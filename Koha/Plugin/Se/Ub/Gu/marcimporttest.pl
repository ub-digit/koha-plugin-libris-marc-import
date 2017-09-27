package Koha::Plugin::Se::Ub::Gu::MarcImport;
use Cwd qw(realpath);
use File::Basename;
my $cwd = dirname(realpath($0));
require "$cwd/MarcImport.pm";

use MARC::Record; #??
sub _RecordsFromMARCXMLFile {
    my ( $filename, $encoding ) = @_;
    my $batch = MARC::File::XML->in( $filename );
    my ( @marcRecords, @errors, $record );
    do {
        eval { $record = $batch->next( $encoding ); };
        if ($@) {
            push @errors, $@;
        }
        push @marcRecords, $record if $record;
    } while( $record );
    return (\@errors, \@marcRecords);
}

#my $testfile = '/home/vagrant/postwith949.xml';
#my $marc_records = _RecordsFromMARCXMLFile($testfile, 'UTF-8');
#my $plugin = __PACKAGE__->new({});
#my $records = $plugin->to_marc({ data => $marc_records });
#use Data::Dumper;
#print $records->[0]->as_xml();

#my $bin_marc_file = '/home/vagrant/SMALL.GUB.ADJUSTED.ITEMS.20161204.marc';
my $bin_marc_file = '/home/vagrant/SMALL.GUB.ADJUSTED.TEST.marc';
my $bin_marc = undef;
{
    local $/ = undef;
    open FILE, $bin_marc_file;
    binmode FILE, ':raw';
    $bin_marc = <FILE>;
    close FILE;
}

my $plugin = __PACKAGE__->new({});
$bin_marc = $plugin->to_marc({ data => $bin_marc, 'debug' => 1 });
use Data::Dumper;
{
    open FILE, ">>", "/tmp/marcrecord";
    binmode FILE;
    print FILE $bin_marc;
    close(FILE);
}
#print $bin_marc;
