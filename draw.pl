#!/usr/local/bin/perl
use strict;
use warnings;

use GD::Graph::bars;
use GD::Graph::hbars;
use Data::Dump;

my $success_file = 'test/success.log';
open(my $fh, '<:encoding(UTF-8)', $success_file)
  or die "Could not open file '$success_file' $!";

my @acquire_events;

my $debug = 1;

while(my $row = <$fh>) {
    chomp $row;

    if($row=~/([\d:]{8},\d+).+(transaction.\d+)\stry\sto\sacquire\s(\w+_LOCK).+on\s(.+)/){
        my @event = (0)*10;

        my $transaction = $2;
        my $page = $4;
        my $lock = $3;
        my $start_time = $1;

        $event[0] = $transaction;
        $event[1] = $page;
        $event[2] = $lock;
        $event[3] = $start_time;

        push(@acquire_events, \@event);

        if ($debug) {
            my $count = scalar @acquire_events;
            print "row: $row\n";
            print "total: $count\n";
        }
    }

    if($row=~/([\d:]{8},\d+).+(transaction.\d+)\ssuccess\sacquire\s(\w+_LOCK).+on\s(.+)/){
        my $transaction = $2;
        my $page = $4;
        my $lock = $3;
        my $endtime = $1;
        my $result = "success";

        for (@acquire_events) {
            my @event = @$_;
            if (!$event[5]){
                if (($event[0] eq $transaction) && ($event[1] eq $page) && ($event[2] eq $lock)) {
                    @$_[4] = $endtime;
                    @$_[5] = $result;
                }
            }
        }
    }

    if($row=~/([\d:]{8},\d+).+acquire\slock\sfailed.+(transaction.\d+),\s(.+),\s(\w+_LOCK)/){
        my $transaction = $2;
        my $page = $3;
        my $lock = $4;
        my $endtime = $1;
        my $result = "fail";

        if ($debug) {
            print "find failed: $row\n";
        }

        for (@acquire_events) {
            my @event = @$_;
            if (!$event[5]){
                if (($event[0] eq $transaction) && ($event[1] eq $page) && ($event[2] eq $lock)) {
                    @$_[4] = $endtime;
                    @$_[5] = $result;
                }
            }
        }
    }
}

if ($debug) {
    print "final:\n";
    dd \@acquire_events;
}

my $success = 0;
my $fail = 0;
my $sunken = 0;
for (@acquire_events) {
    my @event = @$_;
    if ($event[5] eq "success"){
        $success++;
    } elsif ($event[5] eq "fail"){
        $fail++;
    } else {
        $sunken++;
    }
}

print "success: $success\n";
print "fail: $fail\n";
print "sunken: $sunken\n";
my $count = scalar @acquire_events;
print "total: $count";