import com.hft.admin.AdminServer;
import com.hft.config.AppConfig;
import com.hft.config.Env;
import com.hft.control.BotController;
import com.hft.persistence.SqliteStateStore;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;

/** Безопасность админ-API: токен обязателен, CORS только для своей админки, адрес — localhost, адреса бирж — https/wss. */
public class AdminSecurityCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean throwsIae(Runnable r){ try { r.run(); return false; } catch (IllegalArgumentException e) { return true; } }
  static final HttpClient http = HttpClient.newHttpClient();
  static HttpResponse<String> get(int port, String path, String... headers) throws Exception {
    var b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET();
    for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }
  static int freePort() throws Exception { try (var s = new ServerSocket(0)) { return s.getLocalPort(); } }

  public static void main(String[] a) throws Exception {
    // ───── токен генерируется, если не задан, и пишется в .env (сам токен в лог не попадает)
    AppConfig loaded = AppConfig.load();
    ck("token generated when missing", loaded.adminToken().length() == 64);
    ck("token written to .env", Files.readString(Env.file()).contains("ADMIN_TOKEN=" + loaded.adminToken()));
    ck("default bind is localhost", loaded.adminBind().equals("127.0.0.1"));
    ck("second load reuses token", AppConfig.load().adminToken().equals(loaded.adminToken()));

    // ───── запросы: без токена — 401, с токеном — 200; Bearer тоже
    int port = freePort();
    var cfg = AppConfig.defaults().withToken("s3cr3t-token").withPort(port);
    var admin = new AdminServer(cfg, new BotController(cfg, new SqliteStateStore()));
    admin.start();
    try {
      ck("no token -> 401", get(port, "/control/status").statusCode() == 401);
      ck("wrong token -> 401", get(port, "/control/status", "X-Admin-Token", "s3cr3t-tokeX").statusCode() == 401);
      ck("token prefix -> 401", get(port, "/control/status", "X-Admin-Token", "s3cr3t").statusCode() == 401);
      ck("right token -> 200", get(port, "/control/status", "X-Admin-Token", "s3cr3t-token").statusCode() == 200);
      ck("bearer -> 200", get(port, "/control/status", "Authorization", "Bearer s3cr3t-token").statusCode() == 200);
      ck("metrics need token", get(port, "/metrics").statusCode() == 401);
      // CORS: чужой сайт не получает разрешения, своя админка (localhost, файл) — получает
      var evil = get(port, "/control/status", "X-Admin-Token", "s3cr3t-token", "Origin", "https://evil.example");
      ck("cors: foreign origin not allowed", evil.headers().firstValue("Access-Control-Allow-Origin").isEmpty());
      ck("cors: never wildcard", evil.headers().allValues("Access-Control-Allow-Origin").stream().noneMatch("*"::equals));
      ck("cors: localhost panel allowed", get(port, "/control/status", "X-Admin-Token", "s3cr3t-token", "Origin", "http://localhost:8000")
          .headers().firstValue("Access-Control-Allow-Origin").orElse("").equals("http://localhost:8000"));
      ck("cors: opened file allowed", get(port, "/control/status", "X-Admin-Token", "s3cr3t-token", "Origin", "null")
          .headers().firstValue("Access-Control-Allow-Origin").orElse("").equals("null"));
      ck("cors: lookalike host rejected", get(port, "/control/status", "X-Admin-Token", "s3cr3t-token", "Origin", "http://localhost.evil.example")
          .headers().firstValue("Access-Control-Allow-Origin").isEmpty());
    } finally { admin.stop(); }

    // ───── empty token: доступа нет вообще; явный список CORS
    int port2 = freePort();
    var cfg2 = AppConfig.defaults().withToken("").withPort(port2).withCorsOrigins(java.util.List.of("https://admin.example.com"));
    var admin2 = new AdminServer(cfg2, new BotController(cfg2, new SqliteStateStore()));
    admin2.start();
    try {
      ck("blank token -> no access", get(port2, "/control/status").statusCode() == 401 && get(port2, "/control/status", "X-Admin-Token", "").statusCode() == 401);
      ck("cors list: allowed origin", get(port2, "/control/status", "Origin", "https://admin.example.com").headers().firstValue("Access-Control-Allow-Origin").isPresent());
      ck("cors list: localhost no longer implicit", get(port2, "/control/status", "Origin", "http://localhost:8000").headers().firstValue("Access-Control-Allow-Origin").isEmpty());
    } finally { admin2.stop(); }

    // ───── адреса бирж: только https/wss, открытые — только localhost
    ck("url: https ok", !throwsIae(() -> BotController.requireSecureUrl("restUrl", "https://api.binance.com")));
    ck("url: wss ok", !throwsIae(() -> BotController.requireSecureUrl("wsUrl", "wss://stream.bybit.com/v5/public/spot")));
    ck("url: http remote rejected", throwsIae(() -> BotController.requireSecureUrl("restUrl", "http://api.binance.com")));
    ck("url: ws remote rejected", throwsIae(() -> BotController.requireSecureUrl("wsUrl", "ws://evil.example/ws")));
    ck("url: http localhost ok", !throwsIae(() -> BotController.requireSecureUrl("restUrl", "http://127.0.0.1:8545")));
    ck("url: ws localhost ok", !throwsIae(() -> BotController.requireSecureUrl("wsUrl", "ws://localhost:9000/ws")));
    ck("url: empty ok", !throwsIae(() -> BotController.requireSecureUrl("wsUrl", "")));
    ck("url: lookalike localhost rejected", throwsIae(() -> BotController.requireSecureUrl("restUrl", "http://localhost.evil.example")));

    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
