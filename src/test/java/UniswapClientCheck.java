import com.hft.config.*; import com.hft.exchange.uniswapv2.*; import com.hft.model.OrderResult; import com.hft.model.OrderEnums.*;
import com.hft.rest.*; import com.hft.store.*; import com.sun.net.httpserver.*;
import java.math.BigInteger; import java.net.InetSocketAddress; import java.util.*; import com.fasterxml.jackson.databind.*;

public class UniswapClientCheck {
  static int pass, fail; static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static String sel(String sig){ return K.hex(K.keccak256(sig.getBytes())).substring(0,8); }
  static final String WETH="0x00000000000000000000000000000000000000e1", USDC="0x00000000000000000000000000000000000000c1", ROUTER="0x0000000000000000000000000000000000000f01";
  static Map<String,BigInteger> bal = new HashMap<>(); static BigInteger allowance = BigInteger.ZERO; static List<String> txs = new ArrayList<>();
  static String w(BigInteger n){ String h=n.toString(16); return "0".repeat(64-h.length())+h; }
  static BigInteger arg(String data, int i){ return new BigInteger(data.substring(10+64*i, 10+64*(i+1)), 16); }
  // цена 2000 USDC за WETH; USDC 6 знаков, WETH 18
  static BigInteger out(BigInteger in, boolean quoteIn){ return quoteIn ? in.multiply(BigInteger.TEN.pow(12)).divide(BigInteger.valueOf(2000)) : in.multiply(BigInteger.valueOf(2000)).divide(BigInteger.TEN.pow(12)); }
  static BigInteger inFor(BigInteger out, boolean quoteIn){ return quoteIn ? out.multiply(BigInteger.valueOf(2000)).divide(BigInteger.TEN.pow(12)) : out.multiply(BigInteger.TEN.pow(12)).divide(BigInteger.valueOf(2000)); }

  public static void main(String[] a) throws Exception {
    // селекторы независимым keccak
    ck("sel swapExact", sel("swapExactTokensForTokens(uint256,uint256,address[],address,uint256)").equals("38ed1739"));
    ck("sel swapForExact", sel("swapTokensForExactTokens(uint256,uint256,address[],address,uint256)").equals("8803dbee"));
    ck("sel amountsOut", sel("getAmountsOut(uint256,address[])").equals("d06ca61f"));
    ck("sel amountsIn", sel("getAmountsIn(uint256,address[])").equals("1f00ca74"));
    ck("sel approve", sel("approve(address,uint256)").equals("095ea7b3"));
    ck("sel balanceOf", sel("balanceOf(address)").equals("70a08231"));
    ck("sel allowance", sel("allowance(address,address)").equals("dd62ed3e"));

    bal.put(WETH, new BigInteger("1000000000000000000")); bal.put(USDC, new BigInteger("1000000000")); // 1 WETH, 1000 USDC
    ObjectMapper m = new ObjectMapper();
    HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    srv.createContext("/", ex -> {
      JsonNode req = m.readTree(ex.getRequestBody().readAllBytes()); String method = req.path("method").asText(); JsonNode p = req.path("params"); String res;
      switch (method) {
        case "eth_chainId": res="\"0x1\""; break;
        case "eth_getTransactionCount": res="\"0x5\""; break;
        case "eth_gasPrice": res="\"0x3b9aca00\""; break;
        case "eth_estimateGas": res="\"0x30d40\""; break;
        case "eth_getBalance": res="\"0xde0b6b3a7640000\""; break;
        case "eth_getTransactionReceipt": res="{\"status\":\"0x1\"}"; break;
        case "eth_sendRawTransaction": { String raw=p.get(0).asText(); txs.add(raw); String data=raw.split("\\|")[4]; String s8=data.substring(2,10);
          if (s8.equals("095ea7b3")) allowance = arg(data,1);
          else { BigInteger A=arg(data,0), B=arg(data,1); boolean exact = s8.equals("38ed1739");
            // путь: адрес первого элемента
            String first = data.substring(10+64*5+64, 10+64*5+128).substring(24); boolean quoteIn = first.equals(USDC.substring(2));
            BigInteger in = exact ? A : inFor(A, quoteIn), o2 = exact ? out(A, quoteIn) : A;
            bal.merge(quoteIn?USDC:WETH, in.negate(), BigInteger::add); bal.merge(quoteIn?WETH:USDC, o2, BigInteger::add); }
          res="\"0xabcdef0000000000000000000000000000000000000000000000000000000001\""; break; }
        case "eth_call": { String data=p.get(0).path("data").asText(), to=p.get(0).path("to").asText(); String s8=data.substring(2,10);
          if (s8.equals("70a08231")) res="\"0x"+w(bal.get(to))+"\"";
          else if (s8.equals("dd62ed3e")) res="\"0x"+w(allowance)+"\"";
          else { BigInteger amt=arg(data,0); String first=data.substring(10+64*3, 10+64*4).substring(24); boolean quoteIn=first.equals(USDC.substring(2));
            boolean isOut = s8.equals("d06ca61f"); BigInteger other = isOut ? out(amt,quoteIn) : inFor(amt,quoteIn);
            res="\"0x"+w(BigInteger.valueOf(32))+w(BigInteger.TWO)+w(isOut?amt:other)+w(isOut?other:amt)+"\""; }
          break; }
        default: res="null";
      }
      byte[] bt=("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":"+res+"}").getBytes(); ex.sendResponseHeaders(200,bt.length); ex.getResponseBody().write(bt); ex.close(); });
    srv.start(); String url="http://127.0.0.1:"+srv.getAddress().getPort();
    var f = new SymbolFilters(); var tc = new TC();
    var c = new UniswapV2Client(new ExchangeConfig("uniswapv2", false,url,"",5000,List.of("WETHUSDC"),20,100), new Credentials("0xaa","key"), f, tc,
        ROUTER, "WETH="+WETH+":18;USDC="+USDC+":6", "0.5");
    c.loadFilters(List.of("WETHUSDC"));
    // покупка на 100 USDC
    OrderResult r = c.buyMarketForQuote("WETHUSDC", 100);
    ck("buy filled", r.isFilled() && Math.abs(r.executedQty()-0.05)<1e-9 && Math.abs(r.avgPrice()-2000)<1e-6);
    ck("approve then swap", txs.size()==2 && txs.get(0).contains("|"+USDC+"|0x095ea7b3") && txs.get(1).contains("|"+ROUTER+"|0x38ed1739"));
    ck("approve exact amount", allowance.equals(new BigInteger("100000000")));
    String swap = txs.get(1).split("\\|")[4];
    ck("amountIn", arg(swap,0).equals(new BigInteger("100000000")));
    BigInteger expectOut = new BigInteger("50000000000000000"); BigInteger minOut = expectOut.multiply(BigInteger.valueOf(995)).divide(BigInteger.valueOf(1000));
    ck("minOut slippage", arg(swap,1).equals(minOut));
    ck("path offset 0xa0", arg(swap,2).equals(BigInteger.valueOf(160)) && arg(swap,3).equals(new BigInteger("aa",16)));
    ck("chain+nonce", txs.get(1).startsWith("0xRAW|1|5|"));
    // продажа 0.05 WETH
    r = c.sellMarket("WETHUSDC", 0.05);
    ck("sell filled", r.isFilled() && Math.abs(r.executedQty()-0.05)<1e-9 && Math.abs(r.avgPrice()-2000)<1e-6 && r.side()==Side.SELL);
    // IOC лимитка вне цены -> без транзакций
    int before = txs.size();
    r = c.buyLimit("WETHUSDC", 0.05, 1900, TimeInForce.IOC);
    ck("limit not marketable", r.isRejected() && txs.size()==before);
    r = c.buyLimit("WETHUSDC", 0.05, 2100, TimeInForce.IOC);
    ck("limit marketable", r.isFilled() && txs.size()>before);
    boolean t=false; try{ c.buyLimit("WETHUSDC", 0.05, 2100, TimeInForce.GTC);}catch(UnsupportedOperationException e){t=true;} ck("gtc unsupported", t);
    t=false; try{ c.cancelOrder("WETHUSDC", 1);}catch(UnsupportedOperationException e){t=true;} ck("cancel unsupported", t && c.cancelAll("WETHUSDC")==0);
    var bs = new BalanceStore(); c.loadBalances(bs);
    ck("balances", bs.free("ETH")==1.0 && bs.free("USDC")>0 && bs.free("WETH")>0);
    srv.stop(0); System.out.println("passed="+pass+" failed="+fail);
  }
}
