import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/** Минимальный WebSocket-сервер для тестов: рукопожатие, маскированные кадры клиента, text/binary/ping/close. */
public class MiniWsServer implements Closeable {
  public final ServerSocket ss;
  public final List<Conn> conns = new CopyOnWriteArrayList<>();
  public final List<String> received = new CopyOnWriteArrayList<>();
  public volatile BiConsumer<Conn,String> onText = (c, t) -> {};
  public volatile java.util.function.Consumer<Conn> onOpen = c -> {};
  public final java.util.concurrent.atomic.AtomicInteger pongs = new java.util.concurrent.atomic.AtomicInteger();
  public volatile boolean accepting = true;

  public MiniWsServer() throws IOException { this(0); }

  /** Сервер на заданном порту (0 — любой свободный). */
  public MiniWsServer(int port) throws IOException {
    ss = new ServerSocket(); ss.setReuseAddress(true); ss.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port), 50);
    Thread t = new Thread(() -> { while (!ss.isClosed()) { try { Socket s = ss.accept(); new Thread(() -> serve(s)).start(); } catch (IOException e) { return; } } });
    t.setDaemon(true); t.start();
  }
  public String url() { return "ws://127.0.0.1:" + ss.getLocalPort() + "/ws"; }

  void serve(Socket s) {
    try {
      InputStream in = s.getInputStream(); OutputStream out = s.getOutputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.ISO_8859_1));
      String line, key = null;
      while ((line = br.readLine()) != null && !line.isEmpty())
        if (line.toLowerCase().startsWith("sec-websocket-key:")) key = line.substring(18).trim();
      if (!accepting || key == null) { s.close(); return; }
      String acc = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes()));
      out.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + acc + "\r\n\r\n").getBytes());
      out.flush();
      Conn c = new Conn(s, out);
      conns.add(c);
      onOpen.accept(c);
      DataInputStream d = new DataInputStream(s.getInputStream());   // BufferedReader мог съесть байты? клиент ждёт 101 до отправки, так что нет
      while (true) {
        int b0 = d.read(); if (b0 < 0) break;
        int b1 = d.readUnsignedByte();
        int op = b0 & 0x0f; long len = b1 & 0x7f;
        if (len == 126) len = d.readUnsignedShort(); else if (len == 127) len = d.readLong();
        byte[] mask = new byte[4]; if ((b1 & 0x80) != 0) d.readFully(mask);
        byte[] p = new byte[(int) len]; d.readFully(p);
        for (int i = 0; i < p.length; i++) p[i] ^= mask[i % 4];
        if (op == 1) { String t = new String(p, StandardCharsets.UTF_8); received.add(t); onText.accept(c, t); }
        else if (op == 9) c.frame(10, p);
        else if (op == 10) pongs.incrementAndGet();
        else if (op == 8) { c.frame(8, p); break; }
      }
    } catch (Exception e) { /* закрыто */ }
    try { s.close(); } catch (IOException ignored) {}
  }

  public static class Conn {
    final Socket s; final OutputStream out;
    Conn(Socket s, OutputStream out) { this.s = s; this.out = out; }
    synchronized void frame(int op, byte[] p) { frameRaw(0x80 | op, p); }
    synchronized void frameRaw(int b0, byte[] p) {
      try {
        out.write(b0);
        if (p.length < 126) out.write(p.length);
        else if (p.length < 65536) { out.write(126); out.write(p.length >> 8); out.write(p.length); }
        else { out.write(127); new DataOutputStream(out).writeLong(p.length); }
        out.write(p); out.flush();
      } catch (IOException e) { /* клиент ушёл */ }
    }
    public void text(String t) { frame(1, t.getBytes(StandardCharsets.UTF_8)); }
    public void binary(byte[] b) { frame(2, b); }
    public void ping() { frame(9, new byte[]{1}); }
    /** Текст двумя кадрами (фрагментация). */
    public void textFragmented(String t) {
      byte[] b = t.getBytes(StandardCharsets.UTF_8); int h = b.length / 2;
      frameRaw(0x01, Arrays.copyOfRange(b, 0, h));
      frameRaw(0x80, Arrays.copyOfRange(b, h, b.length));
    }
    public void closeSocket() { try { s.close(); } catch (IOException ignored) {} }
  }

  @Override public void close() { try { ss.close(); } catch (IOException ignored) {} for (Conn c : conns) c.closeSocket(); }
}
