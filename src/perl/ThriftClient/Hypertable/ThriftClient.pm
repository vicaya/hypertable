require 5.6.0;
use strict;
use warnings;

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

use Hypertable::ThriftGen::Types;
use Hypertable::ThriftGen2::HqlService;

package Hypertable::ThriftClient;
use base('Hypertable::ThriftGen2::HqlServiceClient');

sub new {
  my ($class, $host, $port, $timeout_ms, $do_open) = @_;
  $timeout_ms = 20000 if !$timeout_ms;
  my $socket = new Thrift::Socket($host, $port);
  $socket->setSendTimeout($timeout_ms);
  $socket->setRecvTimeout($timeout_ms);
  my $transport = new Thrift::FramedTransport($socket);
  my $protocol = new Thrift::BinaryProtocol($transport);
  my $self = $class->SUPER::new($protocol);
  $self->{transport} = $transport;

  $self->open($timeout_ms) if !defined($do_open) || $do_open;

  return bless($self, $class);
}

sub DESTROY {
  my $self = shift;
  $self->close();
}

sub open {
  my $self = shift;
  $self->{transport}->open();
  $self->{do_close} = 1;
}

sub close {
  my $self = shift;
  $self->{transport}->close() if $self->{do_close};
}

1;
