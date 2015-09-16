# CAPCOG_PostPostProcess
# Fix a problem caused by Script 10 putting ramp emissions for all counties in the 
# ${offnetdbname}.${c}_movesoutput_zone_final table instead of just the ramp emissions
# for county $c
# DJackson, Eastern Research Group, 2015

use warnings;
#system('cls');

print "\n";
print "Beginning SCRIPT 11 at ".(localtime),"\n"; 
print "\n";

# assign the variables
my $countylist = "48021,48053,48055,48209,48453,48491";
my $dbroot = "CAPCOG_2012_SUMMER_SA_14sep15";
my $localoutpath = "C:\\SEE\\LocalGen\\";

@counties = split(/,/, $countylist);

for my $c (@counties){

	$dbname = "${dbroot}_${c}";
	
	$sql="

	USE ${dbname};
	
	CREATE TABLE IF NOT EXISTS crosstabVMTdailyFixed LIKE crosstabVMTdaily;
	TRUNCATE crosstabVMTdailyFixed;
	INSERT INTO crosstabVMTdailyFixed SELECT * FROM crosstabVMTdaily;

	UPDATE TABLE crosstabVMTdailyFixed SET `11`=`11`/2, `21`=`21`/2,`31`=`31`/2, `32`=`32`/2, `41`=`41`/2, 
		`42`=`42`/2, `43`=`43`/2, `51`=`51`/2, `52`=`52`/2, `53`=`53`/2, `54`=`54`/2, `61`=`61`/2, `62`=`62`/2
	WHERE roadTypeID NOT IN (8, 9);
		
	create Table    if not exists ${dbname}.TotalsFixed
	(
		countyID		INTEGER  UNSIGNED NULL DEFAULT NULL,
		fuelTypeID		INTEGER  UNSIGNED NULL DEFAULT NULL,
		pollutantID		INTEGER  UNSIGNED NULL DEFAULT NULL,
		emissKg			DOUBLE   UNSIGNED NULL DEFAULT NULL,
		VMT				DOUBLE   UNSIGNED NULL DEFAULT NULL
	) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

	truncate ${dbname}.TotalsFixed;
	INSERT INTO ${dbname}.TotalsFixed
	SELECT emiss.countyID, emiss.fuelTypeID, emiss.pollutantID, sum(IFNULL(`11`,0)+IFNULL(`21`,0)+IFNULL(`31`,0)+IFNULL(`32`,0)+IFNULL(`41`,0)+IFNULL(`42`,0)+
												  IFNULL(`43`,0)+IFNULL(`51`,0)+IFNULL(`52`,0)+IFNULL(`53`,0)+IFNULL(`54`,0)+IFNULL(`61`,0)+
												  IFNULL(`62`,0)) as EmissKg, myalias.VMT
	FROM ${dbname}.crosstabemissdaily emiss JOIN

	(SELECT vmt.countyID, vmt.fuelTypeID, sum(IFNULL(`11`,0)+IFNULL(`21`,0)+IFNULL(`31`,0)+IFNULL(`32`,0)+IFNULL(`41`,0)+
							  IFNULL(`42`,0)+IFNULL(`43`,0)+IFNULL(`51`,0)+IFNULL(`52`,0)+IFNULL(`53`,0)+
							  IFNULL(`54`,0)+IFNULL(`61`,0)+IFNULL(`62`,0)) as VMT
	FROM ${dbname}.crosstabVMTdailyFixed vmt
	group by countyID, fuelTypeID) myalias 

	ON emiss.countyid=myalias.countyid and emiss.fuelTypeID=myalias.fuelTypeID
	group by countyID, fuelTypeID, pollutantID;

	";
	open(out1,">script.sql");
	print out1 $sql;
	close(out1);
	`mysql --defaults-extra-file=user.cnf < script.sql`;

	print "County ${c} VMT output loaded at ".(localtime),"\n";
	
}

# CREATE NEW SCENARIO-BASED DB FOR HOLDING RESULTS FROM ALL COUNTIES

$summdb = "${dbroot}_summary_fixed";
$sql="drop database if exists ${summdb};
	create Database if not exists ${summdb};
	use ${summdb};

	create table if not exists ${summdb}.CrossTabEmissHourlySummary like ${dbname}.CrossTabEmissHourly;
	create table if not exists ${summdb}.CrossTabEmissDailySummary like ${dbname}.CrossTabEmissDaily;
	create table if not exists ${summdb}.CrossTabVMTHourlySummary like ${dbname}.CrossTabVMTHourly;
	create table if not exists ${summdb}.CrossTabVMTDailySummary like ${dbname}.CrossTabVMTDaily;
	create table if not exists ${summdb}.SummaryTotals like ${dbname}.TotalsFixed;
	CREATE TABLE IF NOT EXISTS ${summdb}.linkSummaryTotals LIKE ${dbname}.linkTotals;

	truncate ${summdb}.CrossTabEmissHourlySummary;
	truncate ${summdb}.CrossTabEmissDailySummary;
	truncate ${summdb}.CrossTabVMTHourlySummary;
	truncate ${summdb}.CrossTabVMTDailySummary;
	truncate ${summdb}.SummaryTotals;
	TRUNCATE ${summdb}.linkSummaryTotals;
";

open(out1,">script.sql");
print out1 $sql;
close(out1);
`mysql --defaults-extra-file=user.cnf < script.sql`;

# Populate summary tables with results from each county
# Exclude results from other counties (this is the part that fixes the problem originating in Script 10)
for my $c (@counties){
	$dbname = "${dbroot}_${c}";
	$sql="
		INSERT INTO ${summdb}.CrossTabEmissHourlySummary SELECT * FROM ${dbname}.CrossTabEmissHourly WHERE countyID=${c};
		INSERT INTO ${summdb}.CrossTabEmissDailySummary SELECT * FROM ${dbname}.CrossTabEmissDaily WHERE countyID=${c};
		INSERT INTO ${summdb}.CrossTabVMTHourlySummary SELECT * FROM ${dbname}.CrossTabVMTHourly WHERE countyID=${c};
		INSERT INTO ${summdb}.CrossTabVMTDailySummary SELECT * FROM ${dbname}.CrossTabVMTDaily WHERE countyID=${c};
		INSERT INTO ${summdb}.SummaryTotals SELECT * FROM ${dbname}.TotalsFixed WHERE countyID=${c};
		INSERT INTO ${summdb}.linkSummaryTotals SELECT * FROM ${dbname}.linkTotals WHERE countyID=${c};
	";

	open(out1,">script.sql");
	print out1 $sql;
	close(out1);
	`mysql --defaults-extra-file=user.cnf < script.sql`;
}

print "\n";
print "Finished SCRIPT 11 at ".(localtime),"\n"; 
print "\n";

