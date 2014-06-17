#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

from thrift.Thrift import *
import hyperthrift.gen.ClientService
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface(hyperthrift.gen.ClientService.Iface):
  """
  HQL service is a superset of Client service

  It adds capability to execute HQL queries to the service
  """
  def hql_exec(self, ns, command, noflush, unbuffered):
    """
    Execute an HQL command

    @param ns - Namespace id

    @param command - HQL command

    @param noflush - Do not auto commit any modifications (return a mutator)

    @param unbuffered - return a scanner instead of buffered results

    Parameters:
     - ns
     - command
     - noflush
     - unbuffered
    """
    pass

  def hql_query(self, ns, command):
    """
    Convenience method for executing an buffered and flushed query

    because thrift doesn't (and probably won't) support default argument values

    @param ns - Namespace

    @param command - HQL command

    Parameters:
     - ns
     - command
    """
    pass

  def hql_exec2(self, ns, command, noflush, unbuffered):
    """
    @see hql_exec

    Parameters:
     - ns
     - command
     - noflush
     - unbuffered
    """
    pass

  def hql_query2(self, ns, command):
    """
    @see hql_query

    Parameters:
     - ns
     - command
    """
    pass


class Client(hyperthrift.gen.ClientService.Client, Iface):
  """
  HQL service is a superset of Client service

  It adds capability to execute HQL queries to the service
  """
  def __init__(self, iprot, oprot=None):
    hyperthrift.gen.ClientService.Client.__init__(self, iprot, oprot)

  def hql_exec(self, ns, command, noflush, unbuffered):
    """
    Execute an HQL command

    @param ns - Namespace id

    @param command - HQL command

    @param noflush - Do not auto commit any modifications (return a mutator)

    @param unbuffered - return a scanner instead of buffered results

    Parameters:
     - ns
     - command
     - noflush
     - unbuffered
    """
    self.send_hql_exec(ns, command, noflush, unbuffered)
    return self.recv_hql_exec()

  def send_hql_exec(self, ns, command, noflush, unbuffered):
    self._oprot.writeMessageBegin('hql_exec', TMessageType.CALL, self._seqid)
    args = hql_exec_args()
    args.ns = ns
    args.command = command
    args.noflush = noflush
    args.unbuffered = unbuffered
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_hql_exec(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = hql_exec_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.e != None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "hql_exec failed: unknown result");

  def hql_query(self, ns, command):
    """
    Convenience method for executing an buffered and flushed query

    because thrift doesn't (and probably won't) support default argument values

    @param ns - Namespace

    @param command - HQL command

    Parameters:
     - ns
     - command
    """
    self.send_hql_query(ns, command)
    return self.recv_hql_query()

  def send_hql_query(self, ns, command):
    self._oprot.writeMessageBegin('hql_query', TMessageType.CALL, self._seqid)
    args = hql_query_args()
    args.ns = ns
    args.command = command
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_hql_query(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = hql_query_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.e != None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "hql_query failed: unknown result");

  def hql_exec2(self, ns, command, noflush, unbuffered):
    """
    @see hql_exec

    Parameters:
     - ns
     - command
     - noflush
     - unbuffered
    """
    self.send_hql_exec2(ns, command, noflush, unbuffered)
    return self.recv_hql_exec2()

  def send_hql_exec2(self, ns, command, noflush, unbuffered):
    self._oprot.writeMessageBegin('hql_exec2', TMessageType.CALL, self._seqid)
    args = hql_exec2_args()
    args.ns = ns
    args.command = command
    args.noflush = noflush
    args.unbuffered = unbuffered
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_hql_exec2(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = hql_exec2_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.e != None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "hql_exec2 failed: unknown result");

  def hql_query2(self, ns, command):
    """
    @see hql_query

    Parameters:
     - ns
     - command
    """
    self.send_hql_query2(ns, command)
    return self.recv_hql_query2()

  def send_hql_query2(self, ns, command):
    self._oprot.writeMessageBegin('hql_query2', TMessageType.CALL, self._seqid)
    args = hql_query2_args()
    args.ns = ns
    args.command = command
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_hql_query2(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = hql_query2_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.e != None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "hql_query2 failed: unknown result");


class Processor(hyperthrift.gen.ClientService.Processor, Iface, TProcessor):
  def __init__(self, handler):
    hyperthrift.gen.ClientService.Processor.__init__(self, handler)
    self._processMap["hql_exec"] = Processor.process_hql_exec
    self._processMap["hql_query"] = Processor.process_hql_query
    self._processMap["hql_exec2"] = Processor.process_hql_exec2
    self._processMap["hql_query2"] = Processor.process_hql_query2

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_hql_exec(self, seqid, iprot, oprot):
    args = hql_exec_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = hql_exec_result()
    try:
      result.success = self._handler.hql_exec(args.ns, args.command, args.noflush, args.unbuffered)
    except hyperthrift.gen.ttypes.ClientException, e:
      result.e = e
    oprot.writeMessageBegin("hql_exec", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_hql_query(self, seqid, iprot, oprot):
    args = hql_query_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = hql_query_result()
    try:
      result.success = self._handler.hql_query(args.ns, args.command)
    except hyperthrift.gen.ttypes.ClientException, e:
      result.e = e
    oprot.writeMessageBegin("hql_query", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_hql_exec2(self, seqid, iprot, oprot):
    args = hql_exec2_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = hql_exec2_result()
    try:
      result.success = self._handler.hql_exec2(args.ns, args.command, args.noflush, args.unbuffered)
    except hyperthrift.gen.ttypes.ClientException, e:
      result.e = e
    oprot.writeMessageBegin("hql_exec2", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_hql_query2(self, seqid, iprot, oprot):
    args = hql_query2_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = hql_query2_result()
    try:
      result.success = self._handler.hql_query2(args.ns, args.command)
    except hyperthrift.gen.ttypes.ClientException, e:
      result.e = e
    oprot.writeMessageBegin("hql_query2", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class hql_exec_args:
  """
  Attributes:
   - ns
   - command
   - noflush
   - unbuffered
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'ns', None, None, ), # 1
    (2, TType.STRING, 'command', None, None, ), # 2
    (3, TType.BOOL, 'noflush', None, False, ), # 3
    (4, TType.BOOL, 'unbuffered', None, False, ), # 4
  )

  def __init__(self, ns=None, command=None, noflush=thrift_spec[3][4], unbuffered=thrift_spec[4][4],):
    self.ns = ns
    self.command = command
    self.noflush = noflush
    self.unbuffered = unbuffered

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.ns = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.command = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.BOOL:
          self.noflush = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.BOOL:
          self.unbuffered = iprot.readBool();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_exec_args')
    if self.ns != None:
      oprot.writeFieldBegin('ns', TType.I64, 1)
      oprot.writeI64(self.ns)
      oprot.writeFieldEnd()
    if self.command != None:
      oprot.writeFieldBegin('command', TType.STRING, 2)
      oprot.writeString(self.command)
      oprot.writeFieldEnd()
    if self.noflush != None:
      oprot.writeFieldBegin('noflush', TType.BOOL, 3)
      oprot.writeBool(self.noflush)
      oprot.writeFieldEnd()
    if self.unbuffered != None:
      oprot.writeFieldBegin('unbuffered', TType.BOOL, 4)
      oprot.writeBool(self.unbuffered)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_exec_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (HqlResult, HqlResult.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'e', (hyperthrift.gen.ttypes.ClientException, hyperthrift.gen.ttypes.ClientException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = HqlResult()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = hyperthrift.gen.ttypes.ClientException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_exec_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.e != None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_query_args:
  """
  Attributes:
   - ns
   - command
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'ns', None, None, ), # 1
    (2, TType.STRING, 'command', None, None, ), # 2
  )

  def __init__(self, ns=None, command=None,):
    self.ns = ns
    self.command = command

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.ns = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.command = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_query_args')
    if self.ns != None:
      oprot.writeFieldBegin('ns', TType.I64, 1)
      oprot.writeI64(self.ns)
      oprot.writeFieldEnd()
    if self.command != None:
      oprot.writeFieldBegin('command', TType.STRING, 2)
      oprot.writeString(self.command)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_query_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (HqlResult, HqlResult.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'e', (hyperthrift.gen.ttypes.ClientException, hyperthrift.gen.ttypes.ClientException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = HqlResult()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = hyperthrift.gen.ttypes.ClientException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_query_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.e != None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_exec2_args:
  """
  Attributes:
   - ns
   - command
   - noflush
   - unbuffered
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'ns', None, None, ), # 1
    (2, TType.STRING, 'command', None, None, ), # 2
    (3, TType.BOOL, 'noflush', None, False, ), # 3
    (4, TType.BOOL, 'unbuffered', None, False, ), # 4
  )

  def __init__(self, ns=None, command=None, noflush=thrift_spec[3][4], unbuffered=thrift_spec[4][4],):
    self.ns = ns
    self.command = command
    self.noflush = noflush
    self.unbuffered = unbuffered

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.ns = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.command = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.BOOL:
          self.noflush = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.BOOL:
          self.unbuffered = iprot.readBool();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_exec2_args')
    if self.ns != None:
      oprot.writeFieldBegin('ns', TType.I64, 1)
      oprot.writeI64(self.ns)
      oprot.writeFieldEnd()
    if self.command != None:
      oprot.writeFieldBegin('command', TType.STRING, 2)
      oprot.writeString(self.command)
      oprot.writeFieldEnd()
    if self.noflush != None:
      oprot.writeFieldBegin('noflush', TType.BOOL, 3)
      oprot.writeBool(self.noflush)
      oprot.writeFieldEnd()
    if self.unbuffered != None:
      oprot.writeFieldBegin('unbuffered', TType.BOOL, 4)
      oprot.writeBool(self.unbuffered)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_exec2_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (HqlResult2, HqlResult2.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'e', (hyperthrift.gen.ttypes.ClientException, hyperthrift.gen.ttypes.ClientException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = HqlResult2()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = hyperthrift.gen.ttypes.ClientException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_exec2_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.e != None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_query2_args:
  """
  Attributes:
   - ns
   - command
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'ns', None, None, ), # 1
    (2, TType.STRING, 'command', None, None, ), # 2
  )

  def __init__(self, ns=None, command=None,):
    self.ns = ns
    self.command = command

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.ns = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.command = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_query2_args')
    if self.ns != None:
      oprot.writeFieldBegin('ns', TType.I64, 1)
      oprot.writeI64(self.ns)
      oprot.writeFieldEnd()
    if self.command != None:
      oprot.writeFieldBegin('command', TType.STRING, 2)
      oprot.writeString(self.command)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class hql_query2_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (HqlResult2, HqlResult2.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'e', (hyperthrift.gen.ttypes.ClientException, hyperthrift.gen.ttypes.ClientException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = HqlResult2()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = hyperthrift.gen.ttypes.ClientException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('hql_query2_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.e != None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
