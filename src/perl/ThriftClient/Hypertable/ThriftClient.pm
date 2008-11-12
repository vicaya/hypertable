require 5.6.0;
use strict;
use warnings;

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

use Hypertable::ThriftGen::Types;
use HqlService;

package Hypertable::ThriftClient;
use base('HqlServiceClient');

sub new {
  my ($class, $host, $port, $timeout_ms, $do_open) = @_;
  my $socket = new Thrift::Socket($host, $port);
  my $transport = new Thrift::FramedTransport($socket);
  my $protocol = new Thrift::BinaryProtocol($transport);
  my $self = $class->SUPER::new($protocol);
  $self->{transport} = $transport;

  $self->open($timeout_ms) if !defined($do_open) || $do_open;

  return bless($self, $class);
}

sub open {
  my ($self, $timeout_ms) = @_;
  $self->{transport}->open();
  $self->{do_close} = 1;
}

sub close {
  my $self = shift;
  $self->{transport}->close() if $self->{do_close};
}

1;
