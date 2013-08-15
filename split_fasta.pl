#! /usr/bin/perl
use warnings;
use strict;
use Text::Wrap;

## Program Info:
#
# Name:  split_fasta
#
# Function:  Takes a multiple fasta file and removes a set of 
#    sequences to makes a second fasta file.  Useful for pulling
#    subsets of sequences from entire genomes.
#
# Author: John Nash
#  Copyright (c) National Research Council of Canada, 2000-2001,
#  all rights reserved.
#
# Licence: This script may be used freely as long as no fee is charged
#    for use, and as long as the author/copyright attributions
#    are not removed.
#
# History:
#   Version 1.0 (August 1, 2002): first non-beta release.
#   Version 1.1 (3 Nov, 2003): cleaned up code.
#   Version 1.2 (4 Nov 2003): fixed an oops
#           1.3 (10 Dec 2003): option to spit to STDOUT
#           1.4 (2 Feb, 2012): remove | from filenames
##

my $title = "split_fasta.pl";
my $version = "1.4";
my $date = "2012-02-02";
$Text::Wrap::columns = 65;
my $error_msg = "Type \"$title -h\" for help";

# Get and process the command line params:
my @cmdline = process_command_line();

my $pipeout = $cmdline[0];

## Handle input parameters:
## Handle input parameters:
## Does the input sequence exist:  handle errors
# If $ARGV[0] is not blank, test for file's existence:
if (defined $ARGV[0]) {
    unless (-e $ARGV[0]) {
	die("\nError: Sequence file \'$ARGV[0]\' does *not* exist. \n",
	    $error_msg, "\n");
    }
    open (FILE, $ARGV[0]);
}

# If it has come in from a redirection or pipe, 
#  check it is bigger than 0:
else {
    my $fh = *STDIN;
    unless ((-p $fh) or (-s $fh)) {
	die("\nError: Piped sequence file does *not* exist. \n",
	    $error_msg, "\n");
    }
    *FILE = *STDIN;
}


# Massage each sequence from the FASTA file to a string:
my (%seq_name, %seq_str);

# read in the sequence from a FASTA file, from stdin:
my $count = 0;  

while (<FILE>) {
    
# Substitutes DOS textfile carriage returns with Unix ones:
    s/\r\n/\n/g;
    chomp;
    
# If the line begins with a ">", it's a comment field.
# Therefore we need the name:
    if (/^>/)  {
	
# Split on the SPACE character or else we could have HUGE file names:
	my (@temp) = split / /;
	
# Replace the "|" character with "_".   
	$temp[0] =~ s/\|/\_/g;
# Remove trailing underscores.
	$temp[0] =~ s/\_\Z//g;
	
# Add it to the array/
	$seq_name{$count} = $temp[0];
	$count++;
    }
    else {
	$seq_str{$count - 1} .= $_; 
    }
} # end of while

my @dupes = ();

# sort each value alphabetically, and look for duplicate names:
my @namelist = sort { $a cmp $b } values %seq_name;
for ($count = 1; $count <= $#namelist; $count++)  {
    if ($namelist[$count] eq $namelist[$count - 1])  {
	push @dupes, $namelist[$count];
    }
}

# remove the additional duplicate names:
my %dupes = ();
foreach (@dupes) {
    $dupes{$_}++;
}
@dupes = keys %dupes;

# find the key/value pair of each copy of the duplicate, and change it:
foreach my $match (@dupes)  {
    $count = 0;
    foreach my $item (keys %seq_name)  {
	if ($seq_name{$item} eq $match)  {
	    $count++;
	    $seq_name{$item} = "$seq_name{$item}" . "_" . "$count";
	}
    }
}

# It's a quick fix, we can be elegant later:
# Print it all out:

foreach (sort {$a <=> $b} keys %seq_name)  {
    if ($pipeout eq "no")	{
	open (OUTFILE, ">$seq_name{$_}.fa") or 
	    die("\nError: Cannot open requested file ($!) \n");	
    }
    else {
	*OUTFILE = *STDOUT;
    }
    print OUTFILE $seq_name{$_}, "\n";
    print OUTFILE wrap('', '', "$seq_str{$_}\n");
}

exit; 

### end of main:

sub help {
print <<EOHelp;
$title $version ($date)
	
Syntax:  $title \"fasta_file\"
   or    $title -h for help
   or    $title -P \"fasta_file\" to send output to STDOUT

   \"$title fasta_file\" will split a multiple fasta file into 
   many, many, many files, each containing one sequence, in fasta 
   format.  It will use the first word after the \"\>\" sign in the
   comment field to name each file. It will rename duplicate entries by 
   appending \"\_1\", \"\_2\", etc, to the filename.

   Genbank delimits fields in fasta headers with the "|" character.  This
   breaks computers as it is an illegal file name.  These get changed 
   to a "_" charchter.

Use the \"-P\" switch to use the program to pipe data out

EOHelp
die ("\n");
} # end of sub help

sub process_command_line {
# Variables:
    my %opts = ();    # command line params, as entered by user
    my @cmd_line;     # returned value
    my @list;         # %opts as an array for handling
    my $cmd_args;	    # return value for getopts()
    my $item;
    my $pipeout="no";
    
# Get the command=line parameters:
    use vars qw($opt_h);
    use Getopt::Std;
    $cmd_args = getopts('hP', \%opts);
    
# Die on illegal argument list:
    if ($cmd_args == 0) {
	die ("Error: Missing or incorrect command line parameter(s)!\n",
	     $error_msg, "\n");
    }
    
# Make the hashes into an array:
    @list = keys %opts;
    
# Do a quick check for "help" and the compulsory parameters:
    foreach $item (@list)  {
# Help:
	if ($item eq "h")  { help(); }
# Pipe out:
	if ($item eq "P")  { $pipeout="yes"; }
    }
    @cmdline=($pipeout);
    return @cmdline;
} #end of sub process_command_line()

