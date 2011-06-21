import sys
import eventlet
#from eventlet import api, coros, event
import logging
import core
import errors
#from .. import core
import socket

logging.basicConfig()
logger = logging.getLogger('rtjp')

class RTJPServer(object):
    logger = logging.getLogger("rtjp_eventlet.RTJPServer")
    def __init__(self, sock=None):
        self._accept_queue = eventlet.queue.Queue()
        self._sock = sock
        if self._sock:
            eventlet.spawn(self._run)
        
    def listen(self, port=None, interface="", sock=None):
        if not (sock or port):
            raise Exception("listen requires either a listening sock or port")
        if sock:
            self._sock = sock
        else:
            self._sock = eventlet.listen((interface, port))
        ev = eventlet.event.Event()
        ev.send(None)
        eventlet.spawn(self._run)
        return ev
        
    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None
        
    def _run(self):
        while True:
            try:
                sock, addr = self._sock.accept()
                self.logger.debug('accepted connection sock: %s, addr: %s', sock, addr)
                self._accept_queue.put(RTJPConnection(sock=sock, addr=addr))
            except socket.error:
                break
            
    def accept(self):
        conn = self._accept_queue.get()
        if isinstance(conn, Exception):
            raise conn
        else:
            return conn

class RTJPConnection(object):
    logger = logging.getLogger('rtjp_eventlet.RTJPConnection')
    def __init__(self, delimiter='\r\n', sock=None, addr=None):
        self.frame_id = 0
        self._frame_queue = eventlet.queue.Queue()
        self.delimiter = delimiter
        self._sock = sock
        self._addr = addr
        self._send_lock = eventlet.semaphore.Semaphore()
        self._active_loop = eventlet.spawn(self._loop)
        
    def connect(self, host, port):
        self._connect(host, port)
        
    def close(self):
        self._cleanup()
        
    def _cleanup(self):
        if self._active_loop:
            loop = self._active_loop
            self._active_loop = None
            eventlet.kill(loop, errors.QuitLoop())
            #eventlet.spawn(loop.throw, errors.QuitLoop()).wait()
        if self._sock:
            sock = self._sock
            self._sock = None
            sock.close()
        
    def _connect(self, host, port):
        sock = eventlet.connect((host, port))
        self._addr = (host, port)
        self._make_connection(sock)
        
    def _loop(self):
        buffer = ""
        while True:
            try:
                data = self._sock.recv(1024)
                self.logger.debug('RECV %s: %s', self, repr(data))
            except errors.QuitLoop:
                break
            except Exception, e:
                self.logger.error('%s Error while reading from self._sock', e, exc_info=True)
                break
            if not data:
                self.logger.debug('no data, breaking...')
                break;
            buffer += data
            while self.delimiter in buffer:
                raw_frame, buffer = buffer.split(self.delimiter, 1)
                try:
                    frame = core.deserialize_frame(raw_frame)
                except core.RTJPParseException, e:
                    self.send_error(e.id, str(e))
                    continue
                self.logger.debug('RECV: %s, %s, %s' % tuple(frame))
                self._frame_queue.put(frame)
        
        self._active_loop = None
        self._cleanup()
        self.logger.debug('put Connection Lost in the queue')
        self._frame_queue.put(errors.ConnectionLost("Connection Lost"), False)

    def recv_frame(self):
        frame = self._frame_queue.get()
        if isinstance(frame, Exception):
            raise frame
        else:
            return frame

    def send_frame(self, name, args={}):
        if not self._sock:
            raise errors.NotConnected()
        self.frame_id += 1
        self.logger.debug('SEND: %s, %s, %s' % (self.frame_id, name, args))
        buffer = core.serialize_frame(self.frame_id, name, args)
        
        if not self._active_loop:
            raise errors.NotConnected()
        try:
            try:
                self._send_lock.acquire()
                self._sock.sendall(buffer)
                self.logger.debug('SENT %s: %s', self, repr(buffer))
            except Exception, e:
                self.logger.exception('%s Error sending payload %s', self, repr(buffer))
                try:
                    self._sock.shutdown()
                    self._sock.close()
                except:
                    pass
                raise
            else:
                return id
        finally:
            self._send_lock.release()
        
    def send_error(self, reference_id, msg):
        self.send_frame('ERROR', { 'id': reference_id, 'msg': str(msg) })
