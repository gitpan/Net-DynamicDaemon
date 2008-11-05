package Net::DynamicDaemon;
use strict;
use warnings;
use UNIVERSAL qw(isa);
use Carp qw(croak confess);
use IO::Socket::INET;
use POSIX ":sys_wait_h";
#use Smart::Comments;

our $VERSION = '0.03';

# Net::DynamicDaemon->run({
#     -pidfile       => '/tmp/foo.pid', # optional
#     -port          => 80,
#     -min_procs     => 2,              # default: 1
#     -max_procs     => 10,             # default: 100
#     -on_request    => sub { my $socket = shift; ... }
#     -max_requests  => 100,            # default: 0
#     -no_fork       => 0,              # default: 0
#     -on_idle       => sub { }         # optional
#     -idle_interval => 5,              # default : 1
#   });
sub run {
    my $class = shift;
    my $args  = shift;

    if (!isa $args => 'HASH') {
        croak "Usage: " . __PACKAGE__ . "->run({ arguments... });";
    }

    if (!defined $args->{-port}) {
        croak 'argument -port is required';
    }
    elsif ($args->{-port} !~ m/^\d+$/) {
        croak "argument -port must be a non-negative integer: $args->{-port}";
    }

    if (defined (my $min_procs = $args->{-min_procs})) {
        if ($min_procs !~ m/^\d+$/ || $min_procs == 0) {
            croak "argument -min_procs must be a natural number: $min_procs";
        }
    }
    else {
        $args->{-min_procs} = 1;
    }

    if (defined (my $max_procs = $args->{-max_procs})) {
        if ($max_procs !~ m/\d+$/) {
            croak "argument -max_procs must be an integer: $max_procs";
        }
        elsif ($max_procs < $args->{-min_procs}) {
            croak "argument -max_procs must be greater than or equal to -min_procs: $max_procs";
        }
    }
    else {
        $args->{-max_procs} = 100;
    }

    if (!defined $args->{-on_request}) {
        croak 'argument -on_request is required';
    }
    elsif (!isa $args->{-on_request} => 'CODE') {
        croak "argument -on_request must be a function: $args->{-on_request}";
    }

    if (defined (my $max_requests = $args->{-max_requests})) {
        if ($max_requests !~ m/\d+$/) {
            croak "argument -max_requests must be a non-negative integer: $max_requests";
        }
    }
    else {
        $args->{-max_requests} = 0;
    }

    if (defined $args->{-on_idle}) {
        if (!isa $args->{-on_idle} => 'CODE') {
            croak "argument -on_idle must be a function: $args->{-on_idle}";
        }
    }

    if (defined (my $idle_interval = $args->{-idle_interval})) {
        if ($idle_interval !~ m/\d+$/ || $idle_interval == 0) {
            croak "argument -idle_interval must be a natural number: $idle_interval";
        }
    }
    else {
        $args->{-idle_interval} = 1;
    }

    # write pid file
    if (defined (my $pidfile = $args->{-pidfile})) {
        _write_pid_file($pidfile);
    }

    # start listening
    my $socket = _listen($args->{-port});

    # start parent loop
    if ($args->{-no_fork}) {
        _no_fork_loop({
            -socket        => $socket,
            -on_request    => $args->{-on_request},
            -on_idle       => $args->{-on_idle},
            -idle_interval => $args->{-idle_interval},
        });
    }
    else {
        _parent_loop({
            -socket        => $socket,
            -min_procs     => $args->{-min_procs},
            -max_procs     => $args->{-max_procs},
            -on_request    => $args->{-on_request},
            -on_idle       => $args->{-on_idle},
            -idle_interval => $args->{-idle_interval},
            -max_requests  => $args->{-max_requests},
        });
    }

    # unlink pid file
    if (defined (my $pidfile = $args->{-pidfile})) {
        unlink $pidfile;
    }
}

sub _write_pid_file {
    my $pidfile = shift;

    open my $fh, '>', $pidfile
      or
        confess "Failed to open file $pidfile to write: $!";

    print {$fh} "$$\n";
    close $fh;
}

sub _listen {
    my $port = shift;

    my $socket = IO::Socket::INET->new(
        LocalPort => $port,
        Proto     => 'tcp',
        Type      => SOCK_STREAM,
        Listen    => 10,
        ReuseAddr => 1,
       )
      or
        confess "Failed to start listening to $port/tcp: $!";

    return $socket;
}

sub _no_fork_loop {
    my $args = shift;

    # shutdown flag
    my $shutdown_flag = 0;

    # idle process
    my $last_idle_processed_time = 0;

    # setup signal handlers
    my $shutdown_h   = sub { $shutdown_flag = 1; };
    local $SIG{QUIT} = $shutdown_h;
    local $SIG{TERM} = $shutdown_h;
    local $SIG{HUP}  = $shutdown_h;
    local $SIG{INT}  = $shutdown_h;
    local $SIG{PIPE} = 'IGNORE';
    local $SIG{ALRM} = sub {};
    local $SIG{CHLD} = sub {};

    while (!$shutdown_flag) {
        # call the idle process if necessary
        if ( defined $args->{-on_idle}
             &&
             time - $last_idle_processed_time >= $args->{-idle_interval}
           ) {

            eval {
                $args->{-on_idle}->();
            };
            if ($@) {
                print STDERR $@;
            }
            $last_idle_processed_time = time;
        }

        # wait for a request
        my $fh;
        eval {
            local $SIG{ALRM} = sub {die};
            alarm 1; # timeout
            accept $fh, $args->{-socket};
          }
          or next;

        bless $fh => 'IO::Socket::INET';
        alarm 0;

        # process the request
        eval {
            $args->{-on_request}->($fh);
        };
        if ($@) {
            print STDERR $@;
        }
    }
}

my $busyness = 0;
sub _parent_loop {
    my $args = shift;

    # busyness counter
    #my $busyness = 0;
    $busyness = 0; # To avoid a mysterious problem possibly related to
                   # signal handlers...

    # shutdown flag
    my $shutdown_flag = 0;

    # info of children
    my $last_idle_process_spawned = 0;
    my $idle_pid;
    my @child_pids;

    # setup signal handlers
    my $shutdown_h   = sub { $shutdown_flag = 1; };
    local $SIG{QUIT} = $shutdown_h;
    local $SIG{TERM} = $shutdown_h;
    local $SIG{HUP}  = $shutdown_h;
    local $SIG{INT}  = $shutdown_h;
    local $SIG{USR1} = sub { $busyness++; };
    local $SIG{USR2} = sub { $busyness--; };
    local $SIG{PIPE} = 'IGNORE';
    local $SIG{ALRM} = sub {};
    local $SIG{CHLD} = sub {};

    while (!$shutdown_flag) {
        # spawn child processes as necessary
        _adjust_number_of_processes({
            -child_pids   => \@child_pids,
            -min_procs    => $args->{-min_procs},
            -max_procs    => $args->{-max_procs},
            -busyness     => \$busyness,
            -socket       => $args->{-socket},
            -on_request   => $args->{-on_request},
            -max_requests => $args->{-max_requests},
        });

        # spawn the idle process if necessary
        if ( defined $args->{-on_idle}
             &&
             (!defined $idle_pid || !kill 0, $idle_pid)
             &&
             time - $last_idle_process_spawned >= $args->{-idle_interval}
           ) {
            
            $idle_pid = _spawn_idle_process($args->{-on_idle});
            $last_idle_process_spawned = time;
            alarm $args->{-idle_interval};
        }

        # wait for events
        sleep 1; # If we could block signals, this should just be "sleep;".
        my $pid = waitpid(-1, WNOHANG);
        
        if ($pid != -1) {
            if (defined $idle_pid && $pid == $idle_pid) {
                # the idle process has exited
                $idle_pid = undef;
            }
            else {
                # one of a child has exited
                @child_pids = grep { $_ != $pid } @child_pids;
            }
        }
    }

    # going to shutdown...
    foreach my $pid (@child_pids) {
        kill 'QUIT', $pid;
    }

    if (defined $idle_pid) {
        kill 'QUIT', $idle_pid;
    }

    alarm(0);
}

sub _adjust_number_of_processes {
    my $args = shift;

    while ( @{$args->{-child_pids}} < $args->{-min_procs}
            ||
            ( @{$args->{-child_pids}} < $args->{-max_procs}
              &&
              @{$args->{-child_pids}} - ${$args->{-busyness}} <= 1 ) ) {
        # spawn new process
        push @{$args->{-child_pids}},
          _spawn_process({
              -socket       => $args->{-socket},
              -on_request   => $args->{-on_request},
              -max_requests => $args->{-max_requests},
          });
    }
}

sub _spawn_process {
    my $args = shift;

    my $parent_pid = $$;
    my $child_pid  = fork;
    
    if (!defined $child_pid) {
        confess "Failed to fork: $!";
    }
    elsif ($child_pid == 0) {
        # child
        _reset_sighandlers();

        eval {
            _child_loop({
                -parent_pid   => $parent_pid,
                -socket       => $args->{-socket},
                -on_request   => $args->{-on_request},
                -max_requests => $args->{-max_requests},
            });
        };
        print STDERR $@ if $@;
        exit;
    }
    else {
        # parent
        return $child_pid;
    }
}

sub _child_loop {
    my $args = shift;

    my $request_count = 0;
    while (1) {

        if ( $args->{-max_requests} != 0
             &&
             $request_count >= $args->{-max_requests}
            ) {
            # commit suicide
            last;
        }

        local $SIG{ALRM} = 'IGNORE';
        my $fh;
        eval {
            local $SIG{ALRM} = sub {die};
            alarm 60; # timeout
            accept $fh, $args->{-socket};
          }
          or last;

        bless $fh => 'IO::Socket::INET';
        alarm 0;

        # Tell the parent that a connection is accepted.
        kill 'USR1', $args->{-parent_pid}
          or
            confess "Fail to signal USR1 to the parent pid $args->{-parent_pid}: $!";

        # Process the request.
        eval {
            $args->{-on_request}->($fh);
        };
        my $err = $@;

        # Tell the parent that a request has been (ab)normally finished.
        kill 'USR2', $args->{-parent_pid}
          or
            confess "Fail to signal USR2 to the parent pid $args->{-parent_pid}: $!";

        # Rethrow the exception if any exceptions were arised during the process.
        die $err if $err;

        $request_count++;
    }
}

sub _spawn_idle_process {
    my $on_idle = shift;

    my $pid = fork;
    if (!defined $pid) {
        confess "Failed to fork: $!";
    }
    elsif ($pid == 0) {
        # child
        _reset_sighandlers();
        eval {
            $on_idle->();
        };
        if ($@) {
            print STDERR $@;
        }
        exit;
    }
    else {
        # parent
        return $pid;
    }
}

sub _reset_sighandlers {
    foreach my $signame (qw(QUIT TERM HUP INT USR1 USR2 PIPE ALRM CHLD)) {
        $SIG{$signame} = 'DEFAULT';
    }
}

1;
__END__

=head1 NAME

Net::DynamicDaemon - library for multi-process daemons

=head1 SYNOPSIS

  use Net::DynamicDaemon;
  
  Net::DynamicDaemon->run({
      -pidfile    => 'daemon.pid',
      -port       => 9900,
      -on_request => sub {
          my $fh = shift;
  
          my $line = <$fh>;
          print {$fh} $line;
      },
      -on_idle => sub {
          print "idle\n";
      },
  });

=head1 DESCRIPTION

Net::DynamicDaemon is a library for implementing multi-process server
applications in a very simple way. The number of processes is
automatically adjusted, just like FastCGI.

=head2 C<run>

  Net::DynamicDaemon->run({
      -pidfile       => '/tmp/foo.pid', # optional
      -port          => 80,             # REQUIRED
      -min_procs     => 2,              # optional; defaults to 1
      -max_procs     => 10,             # optional; defaults to 100
      -on_request    => sub {           # REQUIRED
                            my $socket = shift;
                            ...
                        },
      -max_requests  => 100,            # optional; defaults to 100
      -no_fork       => 0,              # optional; defaults to 0
      -on_idle       => sub { }         # optional
      -idle_interval => 5,              # optional; defaults to 1
    });

Open and listen to a TCP socket until the process receives SIGQUIT,
SIGTERM, SIGHUP or SIGINT.

=over 4

=item B<-pidfile>

If this option is present, a PID file will be created at the given
location. The PID file will be automatically deleted when the daemon
quits.

=item B<-port>

The TCP port to which the daemon listens.

=item B<-min_procs>

Minimum number of worker processes, which accepts connections and
processes requests. The number of worker processes decreases down to
this value as the number of incoming requests falls. See L<-max_procs>
and L<-no_fork>.

=item B<-max_procs>

Maximum number of worker processes. The number of worker processes
increases up to this value as the number of incoming requests
raises. See L<-min_procs> and L<-no_fork>.

=item B<-on_request>

The request handler subroutine which takes one argument as an accepted
socket (IO::Handle). The return value of the subroutine is
ignored. The connection will be automatically closed after the end of
execution of handler.

=item B<-max_requests>

Each of worker processes kills itself after processing the given
number of requests, to minimise the fear of memory leaking. (You know,
it is almost impossible to track the source of leaking memory in
Perl. Do not waste your precious time.)

=item B<-no_fork>

Run the daemon in no-fork mode. In this mode, the daemon spawns no
child processes, and processes each requests one by one. Use this
option only for debugging and profiling of your daemons. The options
L<-min_procs>, L<-max_procs> and L<-max_requests> are ignored with
this option enabled.

=item B<-on_idle>

A subroutine which takes no arguments. It will be called periodically
with the given interval. See L<-idle_interval>.

=item B<-idle_interval>

The interval of calling L<-on_idle> in seconds.

=back

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2008 YMIRLINK Inc.

This module is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
