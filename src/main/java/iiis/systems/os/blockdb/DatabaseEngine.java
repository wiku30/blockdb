//Completed: transient log, blocks, integrity check (Untested)
//Todo: recovery, server, test cases

package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.io.File; 
import java.io.FileWriter;
import java.io.FileReader;
import java.util.Scanner;

import javax.lang.model.util.ElementScanner6;

import java.io.IOException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.lang.Iterable;
import java.util.Iterator;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir) {
        instance = new DatabaseEngine(dataDir);
    }

    private HashMap<String, Integer> balances = new HashMap<>();

    private int N=50;

    private int logLength = 0;
    private int numBlocks = 0;
    private int numUpdates = 0;
    private boolean recovery_completed = false;
    private String logPath;
    private String dataDir;
    private FileWriter fw;
    private File fll;

    private long counter=0;

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
        this.logPath = dataDir + "log.txt"; //Type [UserID / FromID ToID] Value [X / Xi]

        //Todo: recovery. Now assume clean start 

        if(isCleanStart())
        {
            try
            {
                fll = new File(logPath);
                fw = new FileWriter(fll, false);
            }
            catch(Exception e)
            {
                System.out.println("Can't create log file:" + logPath);
            }
        }
        else
        {
            recover();
        }

        recovery_completed = true;

        

    }

    public class Tx
    {
        public String Type;
        public String UserID;
        public String FromID;
        public String ToID;
        public int Value;
        public int type;
        public long Random;
        Tx(String Type, String UserID, String FromID, String ToID, int Value, long Random)
        {
            type=3;
            this.Type=Type;
            this.UserID=UserID;
            this.FromID=FromID;
            this.ToID=ToID;
            this.Value=Value;
            this.Random = Random;
        }
        Tx(String Type, String UserID, int Value, long Random)
        {
            type=1;
            this.Type=Type;
            this.UserID=UserID;
            this.FromID="#";
            this.ToID="#";
            this.Value=Value;
            this.Random = Random;
        }
        Tx(String Type, String FromID, String ToID, int Value, long Random)
        {
            type=2;
            this.Type=Type;
            this.UserID="#";
            this.FromID=FromID;
            this.ToID=ToID;
            this.Value=Value;
            this.Random = Random;
        }
        Tx(String Type, String UserID, long Random)
        {
            type=0;
            this.Type=Type;
            this.UserID=UserID;
            this.FromID="#";
            this.ToID="#";
            this.Value=0;
            this.Random = Random;
        }
    }

    private boolean isCleanStart()
    {

        File _log = new File(logPath);
        File _block = new File(dataDir + "1" + ".json");
        if(_log.exists() || _block.exists())
        {
            return false;
        }
        return true;
    }

    private void recover() //assume blocks or log exist
    {
        int MAX = 114514;
        for(int i=0;i<MAX;i++)
        {
            File tmp = new File(dataDir + (i+1) + ".json");
            if(!tmp.exists())
            {
                numBlocks = i;
                break;
            }
        }
        recover_from_blocks();
        recover_from_log();
        System.out.println("logLength: " + logLength + " " + numUpdates);
    }

    private void recover_from_blocks()
    {
        System.out.println("Recovering from blocks...");
        for(int i=1;i<=numBlocks;i++)
        {
            String blpath = dataDir + (i) + ".json";
            System.out.println("Block path:" + blpath);
            File f = new File(blpath);
            System.out.println("Block opened:" + i);
            Scanner scanner = null;
            try
            {
                scanner = new Scanner(f);
                //System.out.println("Scanner:" + i);
                String str = new String(); //full json
                while(scanner.hasNextLine())
                {
                    //System.out.println("One line...");
                    str += scanner.nextLine()+"\n";
                }
                JSONObject jo = JSONObject.parseObject(str);
                JSONArray trans = jo.getJSONArray("Transactions");
                System.out.println("Transactions parsed:" + i);
                Iterator<Object> it = trans.iterator();

                while(it.hasNext())
                {
                    System.out.println("Parsing Tx...");
                    JSONObject obj = (JSONObject) it.next();
                    String tp = obj.getString("Type");
                    String ui = obj.getString("UserID");
                    String fi = obj.getString("FromID");
                    String ti = obj.getString("ToID");
                    int v = obj.getIntValue("Value");
                    long r = obj.getLongValue("TxID");
                    System.out.println("Tx from blocks got: " + r);
                    Tx tx = new Tx(tp,ui,fi,ti,v,r);
                    simulate_Tx(tx);
                }
                System.out.println("Recovery from blocks completed");
            }
            catch(Exception e)
            {
                System.out.println("Corrupted block!");
            }
        }
        
    }

    private void recover_from_log()
    {
        System.out.println("Recovering from log...");
        try
        {
            //System.out.println("############ " + logPath);
            fll = new File(logPath);
            Scanner fr = new Scanner(fll);
            
            while(fr.hasNext())
            {
                int tp = fr.nextInt();
                //System.out.println("############ type:" + tp);
                String Tp = fr.next();
                String UI = fr.next();
                String FI = fr.next();
                String TI = fr.next();
                int V = fr.nextInt();
                long R = fr.nextLong();
                Tx tx = new Tx(Tp,UI,FI,TI,V,R);
                //System.out.println(tp);
                System.out.println(Tp);
                simulate_Tx(tx);
                logLength++;
                numUpdates++;
            }
            fr.close();
            fw = new FileWriter(fll, true);
            System.out.println("Recovering from log: completed");
        }
        catch(Exception e)
        {
            System.out.println("Corrupted log!");
            try
            {
                fw = new FileWriter(fll, false);
            }
            catch(Exception ee)
            {
                System.out.println("Fuck!");
            }
        }
    }


    private int simulate_Tx(Tx tx)
    {
        String t = tx.Type;
        if(t.equals("GET"))
        {
            return get(tx.UserID);
        }
        else if(t.equals("PUT"))
        {
            put(tx.UserID,tx.Value);
        }
        else if(t.equals("DEPOSIT"))
        {
            deposit(tx.UserID,tx.Value);
        }
        else if(t.equals("WITHDRAW"))
        {
            withdraw(tx.UserID,tx.Value);
        }
        else if(t.equals("TRANSFER"))
        {
            transfer(tx.FromID,tx.ToID,tx.Value);
        }
        else
        {
            System.out.println("Invalid Transaction Simulation: \"" + t +"\"");
        }
        return 0;
    }

    private void writeLog(Tx tx)
    {
        if(!recovery_completed)
        {
            return;
        }
        try
        {
            String logstr = tx.type + " " + tx.Type+" "+tx.UserID+" "+tx.FromID+" "+tx.ToID+" "+tx.Value + " " + tx.Random + "\n" ;
            fw.write(logstr);
            fw.flush();
            //System.out.println("Log written:" + logstr);
        }
        catch(Exception e)
        {
            System.out.println("Can't write log!");
        }
        
    }

    private void writeBlock() 
    {
        
        if(!recovery_completed)
        {
            return;
        }
        numBlocks++;
        System.out.println("Writing block: " + numBlocks);
        //
        //
        try
        {
        fw.close();
        File fl = new File(dataDir + numBlocks + ".json");
        FileWriter fw2 = new FileWriter(fl);
        Scanner fr = new Scanner(fll);

        //write blocks
        fw2.write("{\n\"BlockID\":" + numBlocks + ",\n\"PrevHash\":" + "\"00000000\"" + ",\n\"Transactions\":[\n");
        //transactions
        for(int i=0;i<N;i++)
        {
            int tp = fr.nextInt();
            String Tp = fr.next();
            String UI = fr.next();
            String FI = fr.next();
            String TI = fr.next();
            int V = fr.nextInt();
            long R = fr.nextLong();
            fw2.write("{\n");
            fw2.write("\"Type\":\"" + Tp + "\",\n");
            fw2.write("\"UserID\":\"" + UI + "\",\n");
            fw2.write("\"FromID\":\"" + FI + "\",\n");
            fw2.write("\"ToID\":\"" + TI + "\",\n");
            fw2.write("\"Value\":" + V + ",\n");
            fw2.write("\"TxID\":" + R + "\n");
            fw2.write("}");
            if(i<N-1)
            {
                fw2.write(",");
            }
            fw2.write("\n");
        }
        //transactions completed
        fw2.write("],\n");
        fw2.write("\"Nonce\":" + "\"00000000\"\n");
        fw2.write("}");
        //block completed.
        fr.close();
        fw2.close();
        fw = new FileWriter(logPath,false);
        //
        }
        catch(Exception e)
        {
            System.out.println("Can't write block!");
        }
        //
        return;
    }

    private void check() 
    {
        if(numUpdates >= N)
        {
            logLength=0;
            numUpdates=0;

            writeBlock();

            try
            {
                fw = new FileWriter(logPath, false);
            }
            catch(Exception e)
            {
                System.out.println("Failed to check log!");
            }
        }
    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    private long getRandom()
    {
        long randomNum = System.currentTimeMillis();
        //counter++;
        //long randomNum=counter;
        return randomNum;
    }

    public int get(String userId) {
        //logLength++;
        //writeLog(new Tx("GET",userId,getRandom()));
        return getOrZero(userId);
    }

    public boolean put(String userId, int value) {
        if(recovery_completed)
        {
            logLength++;
            numUpdates++;
        }
        Tx tmp = new Tx("PUT",userId,value,getRandom());
        writeLog(tmp);
        balances.put(userId, value);
        check();
        return true;
    }

    public boolean deposit(String userId, int value) {
        if(recovery_completed)
        {
            logLength++;
            numUpdates++;
        }
        writeLog(new Tx("DEPOSIT",userId,value,getRandom()));
        int balance = getOrZero(userId);
        balances.put(userId, balance + value);
        check();
        return true;
    }


    public boolean withdraw(String userId, int value) {

        int balance = getOrZero(userId);
        long rnd = getRandom();
        if(balance >= value)
        {
            if(recovery_completed)
            {
                logLength++;
                numUpdates++;
            }
            writeLog(new Tx("WITHDRAW",userId,value,rnd));
            balances.put(userId, balance - value);
            check();
            return true;
        }
        else
        {
            System.out.println("Transaction "+ rnd + " failed with: Insufficient funds.");
            return false;
        }
    }

    public boolean transfer(String fromId, String toId, int value) {

        int fromBalance = getOrZero(fromId);
        int toBalance = getOrZero(toId);
        long rnd = getRandom();
        if(fromId==toId)
        {
            System.out.println("Transaction "+ rnd + " failed with: Same FromID and ToID.");
            return false;
        }
        else if(fromBalance >= value)
        {
            if(recovery_completed)
            {
                logLength++;
                numUpdates++;
            }
            writeLog(new Tx("TRANSFER",fromId,toId,value,rnd));
            balances.put(fromId, fromBalance - value);
            balances.put(toId, toBalance + value);
            check();
            return true;
        }
        else
        {
            System.out.println("Transaction "+ rnd + " failed with: Insufficient funds.");
            return false;
        }

    }

    public int getLogLength() {
        return logLength;
    }
}
