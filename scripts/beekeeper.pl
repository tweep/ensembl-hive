#!/usr/bin/env perl

use warnings;
use strict;
use DBI;
use Getopt::Long;

use Bio::EnsEMBL::Hive::DBSQL::DBAdaptor;
use Bio::EnsEMBL::Hive::Worker;
use Bio::EnsEMBL::Hive::Queen;
use Bio::EnsEMBL::Hive::URLFactory;
use Bio::EnsEMBL::Hive::DBSQL::AnalysisCtrlRuleAdaptor;

use Bio::EnsEMBL::Hive::Meadow::LSF;
use Bio::EnsEMBL::Hive::Meadow::LOCAL;

main();

sub main {

    $| = 1;
    Bio::EnsEMBL::Registry->no_version_check(1);

        # ok this is a hack, but I'm going to pretend I've got an object here
        # by creating a hash ref and passing it around like an object
        # this is to avoid using global variables in functions, and to consolidate
        # the globals into a nice '$self' package
    my $self = {};

    $self->{'db_conf'} = {
        -host   => '',
        -port   => 3306,
        -user   => 'ensro',
        -pass   => '',
        -dbname => '',
    };

    my ($help, $conf_file);
    my $loopit                      = 0;
    my $sync                        = 0;
    my $local                       = 0;
    my $show_failed_jobs            = 0;
    my $no_pend_adjust              = 0;
    my $worker_limit                = 50;
    my $local_cpus                  = 2;
    my $lsf_options                 = '';
    my $max_loops                   = 0; # not running by default
    my $run                         = 0;
    my $check_for_dead              = 0;
    my $all_dead                    = 0;
    my $remove_analysis_id          = 0;
    my $job_id_for_output           = 0;
    my $show_worker_stats           = 0;
    my $kill_worker_id              = 0;
    my $reset_job_id                = 0;
    my $reset_all_jobs_for_analysis = 0;

    $self->{'sleep_minutes'}        = 2;
#    $self->{'overdue_minutes'}      = 60;   # which means one hour
    $self->{'verbose_stats'}        = 1;
    $self->{'reg_name'}             = 'hive';
    $self->{'maximise_concurrency'} = 0;

    GetOptions('help'              => \$help,

                    # connection parameters
               'conf=s'            => \$conf_file,
               'regfile=s'         => \$self->{'reg_file'},
               'regname=s'         => \$self->{'reg_name'},
               'url=s'             => \$self->{'url'},
               'dbhost=s'          => \$self->{'db_conf'}->{'-host'},
               'dbport=i'          => \$self->{'db_conf'}->{'-port'},
               'dbuser=s'          => \$self->{'db_conf'}->{'-user'},
               'dbpass=s'          => \$self->{'db_conf'}->{'-pass'},
               'dbname=s'          => \$self->{'db_conf'}->{'-dbname'},

                    # loop control
               'loop'              => \$loopit,
               'max_loops=i'       => \$max_loops,
               'run'               => \$run,
               'run_job_id=i'      => \$self->{'run_job_id'},
               'sleep=f'           => \$self->{'sleep_minutes'},

                    # meadow control
               'local!'            => \$local,
               'local_cpus=i'      => \$local_cpus,
               'wlimit=i'          => \$worker_limit,
               'no_pend'           => \$no_pend_adjust,
               'lsf_options=s'     => \$lsf_options,

                    # worker control
               'jlimit=i'          => \$self->{'job_limit'},
               'batch_size=i'      => \$self->{'batch_size'},
               'lifespan=i'        => \$self->{'lifespan'},
               'logic_name=s'      => \$self->{'logic_name'},
               'maximise_concurrency' => \$self->{'maximise_concurrency'},

                    # other commands/options
               'sync'              => \$sync,
               'dead'              => \$check_for_dead,
               'killworker=i'      => \$kill_worker_id,
#               'overdue'           => \$self->{'overdue_minutes'},
               'alldead'           => \$all_dead,
               'no_analysis_stats' => \$self->{'no_analysis_stats'},
               'verbose_stats=i'   => \$self->{'verbose_stats'},
               'worker_stats'      => \$show_worker_stats,
               'failed_jobs'       => \$show_failed_jobs,
               'reset_job_id=i'    => \$reset_job_id,
               'reset_all|reset_all_jobs_for_analysis=s' => \$reset_all_jobs_for_analysis,
               'delete|remove=s'   => \$remove_analysis_id, # careful
               'job_output=i'      => \$job_id_for_output,
               'monitor!'          => \$self->{'monitor'},
    );

    if ($help) { usage(); }

    parse_conf($self, $conf_file);

    if($run or $self->{'run_job_id'}) {
        $max_loops = 1;
    } elsif ($loopit) {
        unless($max_loops) {
            $max_loops = -1; # unlimited
        }
        unless(defined($self->{'monitor'})) {
            $self->{'monitor'} = 1;
        }
    }

    if($self->{'reg_file'}) {
        Bio::EnsEMBL::Registry->load_all($self->{'reg_file'});
        $self->{'dba'} = Bio::EnsEMBL::Registry->get_DBAdaptor($self->{'reg_name'}, 'hive');
    } elsif($self->{'url'}) {
        $self->{'dba'} = Bio::EnsEMBL::Hive::URLFactory->fetch($self->{'url'}) || die("Unable to connect to $self->{'url'}\n");
    } elsif (    $self->{'db_conf'}->{'-host'}
             and $self->{'db_conf'}->{'-user'}
             and $self->{'db_conf'}->{'-dbname'}) { # connect to database specified
                    $self->{'dba'} = new Bio::EnsEMBL::Hive::DBSQL::DBAdaptor(%{$self->{'db_conf'}});
                    $self->{'url'} = $self->{'dba'}->dbc->url;
    } else {
        print "\nERROR : Connection parameters (regfile+regname, url or dbhost+dbuser+dbname) need to be specified\n\n";
        usage();
    }

    my $queen = $self->{'dba'}->get_Queen;
    $queen->{'maximise_concurrency'} = 1 if ($self->{'maximise_concurrency'});
    $queen->{'verbose_stats'} = $self->{'verbose_stats'};

    my $pipeline_name = $self->{'dba'}->get_MetaContainer->list_value_by_key("name")->[0];

    if($local) {
        $self->{'meadow'} = Bio::EnsEMBL::Hive::Meadow::LOCAL->new();
        $self->{'meadow'} -> total_running_workers_limit($local_cpus);
    } else {
        $self->{'meadow'} = Bio::EnsEMBL::Hive::Meadow::LSF->new();
        $self->{'meadow'} -> lsf_options($lsf_options);
    }
    $self->{'meadow'} -> pending_adjust(not $no_pend_adjust);
    $self->{'meadow'} -> submitted_workers_limit($worker_limit);
    $self->{'meadow'} -> pipeline_name($pipeline_name);

    if($reset_job_id) { $queen->reset_and_fetch_job_by_dbID($reset_job_id); }

    if($job_id_for_output) {
        printf("===== job output\n");
        my $job = $self->{'dba'}->get_AnalysisJobAdaptor->fetch_by_dbID($job_id_for_output);
        $job->print_job();
    }

    if($reset_all_jobs_for_analysis) {
        reset_all_jobs_for_analysis($self, $reset_all_jobs_for_analysis)
    }

    if($remove_analysis_id) { remove_analysis_id($self, $remove_analysis_id); }
    if($all_dead)           { $queen->register_all_workers_dead(); }
    if($check_for_dead)     { check_for_dead_workers($self, $queen, 1); }

    if ($kill_worker_id) {
        my $worker = $queen->_fetch_by_hive_id($kill_worker_id);
        if( $self->{'meadow'}->responsible_for_worker($worker)
        and not defined($worker->cause_of_death())) {

            printf("KILL: %10d %35s %15s  %20s(%d) : ", 
                $worker->hive_id, $worker->host, $worker->process_id, 
                $worker->analysis->logic_name, $worker->analysis->dbID);

            $self->{'meadow'}->kill_worker($worker);
            $queen->register_worker_death($worker);
        }
    }

    my $analysis = $self->{'dba'}->get_AnalysisAdaptor->fetch_by_logic_name($self->{'logic_name'});

    if ($max_loops) {

        run_autonomously($self, $max_loops, $queen, $analysis);

    } else {
            # the output of several methods will look differently depending on $analysis being [un]defined

        if($sync) {
            $queen->synchronize_hive($analysis);
        }
        $queen->print_analysis_status($analysis) unless($self->{'no_analysis_stats'});
        $queen->print_running_worker_status;

        show_running_workers($self, $queen) if($show_worker_stats);
        #show_failed_workers($self, $queen);

        $queen->get_num_needed_workers($analysis); # apparently run not for the return value, but for the side-effects
        $queen->get_hive_progress();

        if($show_failed_jobs) {
            print("===== failed jobs\n");
            my $failed_job_list = $self->{'dba'}->get_AnalysisJobAdaptor->fetch_all_failed_jobs();

            foreach my $job (@{$failed_job_list}) {
                $job->print_job();
            }
        }
    }

    if ($self->{'monitor'}) {
        $queen->monitor();
    }

    exit(0);
}

#######################
#
# subroutines
#
#######################

sub usage {
    print "beekeeper.pl [options]\n";
    print "  -help                  : print this help\n";

    print "\n===============[connection parameters]==================\n";
    print "  -conf <path>           : config file describing db connection\n";
    print "  -regfile <path>        : path to a Registry configuration file\n";
    print "  -regname <string>      : species/alias name for the Hive DBAdaptor\n";
    print "  -url <url string>      : url defining where hive database is located\n";
    print "  -dbhost <machine>      : mysql database host <machine>\n";
    print "  -dbport <port#>        : mysql port number\n";
    print "  -dbuser <name>         : mysql connection user <name>\n";
    print "  -dbpass <pass>         : mysql connection password\n";
    print "  -dbname <name>         : mysql database <name>\n";

    print "\n===============[loop control]============================\n";
    print "  -loop                  : run autonomously, loops and sleeps\n";
    print "  -max_loops <num>       : perform max this # of loops in autonomous mode\n";
    print "  -run                   : run 1 iteration of automation loop\n";
    print "  -run_job_id <job_id>   : run 1 iteration for this job_id\n";
    print "  -sleep <num>           : when looping, sleep <num> minutes (default 3min)\n";

    print "\n===============[meadow control]==========================\n";
    print "  -local                 : run jobs on local CPU (fork)\n";
    print "  -local_cpus <num>      : max # workers to be running locally\n";
    print "  -wlimit <num>          : max # workers to create per loop\n";
    print "  -no_pend               : don't adjust needed workers by pending workers\n";
    print "  -lsf_options <string>  : passes <string> to LSF bsub command as <options>\n";

    print "\n===============[worker control]==========================\n";
    print "  -jlimit <num>           : #jobs to run before worker can die naturally\n";
    print "  -batch_size <num>       : #jobs a worker can claim at once\n";
    print "  -lifespan <num>         : lifespan limit for each worker\n";
    print "  -logic_name <string>    : restrict the pipeline stat/runs to this analysis logic_name\n";
    print "  -maximise_concurrency 1 : try to run more different analyses at the same time\n";

    print "\n===============[other commands/options]==================\n";
    print "  -dead                  : clean dead jobs for resubmission\n";
#    print "  -overdue <min>         : worker overdue minutes checking if dead\n";
    print "  -alldead               : all outstanding workers\n";
    print "  -no_analysis_stats     : don't show status of each analysis\n";
    print "  -worker_stats          : show status of each running worker\n";
    print "  -failed_jobs           : show all failed jobs\n";
    print "  -reset_job_id <num>    : reset a job back to READY so it can be rerun\n";
    print "  -reset_all_jobs_for_analysis <logic_name>\n";
    print "                         : reset jobs back to READY so it can be rerun\n";  

    exit(1);  
}

sub parse_conf {
    my ($self, $conf_file) = @_;

  if($conf_file and (-e $conf_file)) {
    #read configuration file from disk
    my @conf_list = @{do $conf_file};

    foreach my $confPtr (@conf_list) {
      #print("HANDLE type " . $confPtr->{TYPE} . "\n");
      if(($confPtr->{TYPE} eq 'COMPARA') or ($confPtr->{TYPE} eq 'DATABASE')) {
        $self->{'db_conf'} = $confPtr;
      }
    }
  }
}

sub check_for_dead_workers {
    my ($self, $queen, $check_buried_in_haste) = @_;

    my $worker_status_hash    = $self->{'meadow'}->status_of_all_my_workers();
    my %worker_status_summary = ();
    my $queen_worker_list     = $queen->fetch_overdue_workers(0);

    print "====== Live workers according to    Queen:".scalar(@$queen_worker_list).", Meadow:".scalar(keys %$worker_status_hash)."\n";

    foreach my $worker (@$queen_worker_list) {
        next unless($self->{'meadow'}->responsible_for_worker($worker));

        my $worker_pid = $worker->process_id();
        if(my $status = $worker_status_hash->{$worker_pid}) { # can be RUN|PEND|xSUSP
            $worker_status_summary{$status}++;
        } else {
            $worker_status_summary{'AWOL'}++;
            $queen->register_worker_death($worker);
        }
    }
    print "\t".join(', ', map { "$_:$worker_status_summary{$_}" } keys %worker_status_summary)."\n\n";

    if($check_buried_in_haste) {
        print "====== Checking for workers buried in haste... ";
        my $buried_in_haste_list = $queen->fetch_dead_workers_with_jobs();
        if(my $bih_number = scalar(@$buried_in_haste_list)) {
            print "$bih_number, reclaiming jobs.\n\n";
            if($bih_number) {
                my $job_adaptor = $queen->db->get_AnalysisJobAdaptor();
                foreach my $worker (@$buried_in_haste_list) {
                    $job_adaptor->reset_dead_jobs_for_worker($worker);
                }
            }
        } else {
            print "none\n";
        }
    }
}

# --------------[worker reports]--------------------

sub show_given_workers {
    my ($self, $worker_list, $verbose_stats) = @_;

    foreach my $worker (@{$worker_list}) {
        printf("%10d %35s(%5d) %5s:%15s %15s (%s)\n", 
            $worker->hive_id,
            $worker->analysis->logic_name,
            $worker->analysis->dbID,
            $worker->beekeeper,
            $worker->process_id, 
            $worker->host,
            $worker->last_check_in);
        printf("%s\n", $worker->output_dir) if ($verbose_stats);
    }
}

sub show_running_workers {
    my ($self, $queen) = @_;

    print("===== running workers\n");
    show_given_workers($self, $queen->fetch_overdue_workers(0), $queen->{'verbose_stats'});
}

sub show_failed_workers {  # does not seem to be used
    my ($self, $queen) = @_;

    print("===== CRASHED workers\n");
    show_given_workers($self, $queen->fetch_failed_workers(), $queen->{'verbose_stats'});
}

sub generate_worker_cmd {
    my $self = shift @_;

    my $worker_cmd = 'runWorker.pl -bk '. $self->{'meadow'}->type();
    if ($self->{'run_job_id'}) {
        $worker_cmd .= " -job_id ".$self->{'run_job_id'};
    } else {
        $worker_cmd .= ((defined $self->{'job_limit'})  ? (' -limit '     .$self->{'job_limit'})  : '')
                    .  ((defined $self->{'batch_size'}) ? (' -batch_size '.$self->{'batch_size'}) : '')
                    .  ((defined $self->{'lifespan'})   ? (' -lifespan '.$self->{'lifespan'}) : '')
                    .  ((defined $self->{'logic_name'}) ? (' -logic_name '.$self->{'logic_name'}) : '')
                    .  ((defined $self->{'maximise_concurrency'}) ? ' -maximise_concurrency 1' : '');
    }

    if ($self->{'reg_file'}) {
        $worker_cmd .= ' -regfile '. $self->{'reg_file'} .' -regname '. $self->{'reg_name'};
    } else {
        $worker_cmd .= ' -url '. $self->{'url'};
    }

    return $worker_cmd;
}

sub get_needed_workers_failed_analyses_resync_if_necessary {
    my ($self, $queen, $this_analysis) = @_;

    my $runCount        = $queen->get_num_running_workers();
    my $load            = $queen->get_hive_current_load();
    my $worker_count    = $queen->get_num_needed_workers($this_analysis);
    my $failed_analyses = $queen->get_num_failed_analyses($this_analysis);

    if($load==0 and $worker_count==0 and $runCount==0) {
        print "*** nothing is running and nothing to do (according to analysis_stats) => perform a hard resync\n" ;

        $queen->synchronize_hive($this_analysis);

        check_for_dead_workers($self, $queen, 1);

        $worker_count    = $queen->get_num_needed_workers($this_analysis);
        $failed_analyses = $queen->get_num_failed_analyses($this_analysis);
        if($worker_count==0) {
            if($failed_analyses==0) {
                print "Nothing left to do".($this_analysis ? (' for analysis '.$this_analysis->logic_name) : '').". DONE!!\n\n";
            }
        }
    }

    return ($worker_count, $failed_analyses);
}


sub run_autonomously {
    my ($self, $max_loops, $queen, $this_analysis) = @_;

    unless(`runWorker.pl`) {
        print("can't find runWorker.pl script.  Please make sure it's in your path\n");
        exit(1);
    }

    my $worker_cmd = generate_worker_cmd($self);

    my $iteration=0;
    my $num_of_remaining_jobs=0;
    my $failed_analyses=0;
    do {
        if($iteration++) {
            $queen->monitor();
            $self->{'dba'}->dbc->disconnect_if_idle;
            printf("sleep %.2f minutes. Next loop at %s\n", $self->{'sleep_minutes'}, scalar localtime(time+$self->{'sleep_minutes'}*60));
            sleep($self->{'sleep_minutes'}*60);  
        }

        print("\n======= beekeeper loop ** $iteration **==========\n");

        check_for_dead_workers($self, $queen, 0);

        $queen->print_analysis_status unless($self->{'no_analysis_stats'});
        $queen->print_running_worker_status;
        #show_failed_workers($self, $queen);

        my $worker_count;
        ($worker_count, $failed_analyses) = get_needed_workers_failed_analyses_resync_if_necessary($self, $queen, $this_analysis);

        if($self->{'run_job_id'}) { # If it's just one job, we don't require more than one worker
                                    # (and we probably do not care about the limits)
            $worker_count = 1;
        } else { # apply different technical and self-imposed limits:
            $worker_count = $self->{'meadow'}->limit_workers($worker_count);
        }

        if($worker_count) {
            print "Submitting $worker_count '".$self->{'meadow'}->type()."' workers\n";

            $self->{'meadow'}->submit_workers($worker_cmd, $worker_count, $iteration);
        } else {
            print "Not submitting any workers this iteration\n";
        }

        # This method prints the progress and returns the number of pending jobs
        $num_of_remaining_jobs = $queen->get_hive_progress();

    } while(!$failed_analyses and $num_of_remaining_jobs and $iteration!=$max_loops);

    printf("dbc %d disconnect cycles\n", $self->{'dba'}->dbc->disconnect_count);
}

sub reset_all_jobs_for_analysis {
    my ($self, $logic_name) = @_;
  
  my $analysis = $self->{'dba'}->get_AnalysisAdaptor->fetch_by_logic_name($logic_name)
      || die( "Cannot AnalysisAdaptor->fetch_by_logic_name($logic_name)"); 
  
  $self->{'dba'}->get_AnalysisJobAdaptor->reset_all_jobs_for_analysis_id($analysis->dbID); 
  $self->{'dba'}->get_Queen->synchronize_AnalysisStats($analysis->stats);
}

sub remove_analysis_id {
    my ($self, $analysis_id) = @_;

    require Bio::EnsEMBL::DBSQL::AnalysisAdaptor or die "$!";

    my $analysis = $self->{'dba'}->get_AnalysisAdaptor->fetch_by_dbID($analysis_id); 

    $self->{'dba'}->get_AnalysisJobAdaptor->remove_analysis_id($analysis->dbID); 
    $self->{'dba'}->get_AnalysisAdaptor->remove($analysis); 
}

