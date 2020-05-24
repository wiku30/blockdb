//Completed: transient log, blocks, integrity check (Untested)
//Todo: recovery, server, test cases

package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.io.File; 
import java.io.FileWriter;
import java.io.FileReader;
import java.util.Scanner;
import java.io.IOException;

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
    private String logPath;
    private String dataDir;
    private FileWriter fw;

    private long counter=0;

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
        this.logPath = dataDir + "log.txt"; //Type [UserID / FromID ToID] Value [X / Xi]

        //Todo: recovery. Now assume clean start 

        try
        {
            fw = new FileWriter(logPath, false);
        }
        catch(Exception e)
        {
            System.out.println("Fuck!");
        }
        

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

    private void writeLog(Tx tx)
    {
        try
        {
            fw.write(tx.type + " " + tx.Type+" "+tx.UserID+" "+tx.FromID+" "+tx.ToID+" "+tx.Value + " " + tx.Random + "\n");
        }
        catch(Exception e)
        {
            System.out.println("Fuck!");
        }
        
    }

    private void writeBlock() 
    {
        numBlocks++;
        //
        //
        try
        {
        fw.close();
        FileWriter fw2 = new FileWriter(dataDir + numBlocks + ".json");
        Scanner fr = new Scanner(logPath);


        //write blocks
        fw2.write("{\n\"BlockID\": " + numBlocks + " ,\n\"PrevHash\": " + "\"00000000\"" + " ,\n\"Transactions\":[\n");
        //transactions
        for(int i=0;i<N;i++)
        {
            int tp = fr.nextInt();
            int Tp = fr.nextInt();
            String UI = fr.next();
            String FI = fr.next();
            String TI = fr.next();
            int V = fr.nextInt();
            long R = fr.nextLong();
            fw2.write("{\n");
            fw2.write("\"Type\": \"" + Tp + " \",\n");
            fw2.write("\"UserID\": \"" + UI + " \",\n");
            fw2.write("\"FromID\": \"" + FI + " \",\n");
            fw2.write("\"ToID\": \"" + TI + " \",\n");
            fw2.write("\"Value\": " + V + " ,\n");
            fw2.write("\"TxID\":" + R + " ,\n");
            fw2.write("}");
            if(i<N-1)
            {
                fw2.write(",");
            }
            fw2.write("\n");
        }
        //transactions completed
        fw2.write("],\n");
        fw2.write("\"Nonce\": " + "\"00000000\"\n");
        fw2.write("}");
        //block completed.
        fr.close();
        fw2.close();
        fw = new FileWriter(logPath,false);
        //
        }
        catch(Exception e)
        {
            System.out.println("Fuck!");
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
                System.out.println("Fuck!");
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
        //long randomNum = System.currentTimeMillis();
        counter++;
        long randomNum=counter;
        return randomNum;
    }

    public int get(String userId) {
        //logLength++;
        //writeLog(new Tx("GET",userId,getRandom()));
        return getOrZero(userId);
    }

    public boolean put(String userId, int value) {
        logLength++;
        numUpdates++;
        Tx tmp = new Tx("PUT",userId,value,getRandom());
        writeLog(tmp);
        balances.put(userId, value);
        check();
        return true;
    }

    public boolean deposit(String userId, int value) {
        logLength++;
        numUpdates++;
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
            logLength++;
            numUpdates++;
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
            logLength++;
            numUpdates++;
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
