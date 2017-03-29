/*
  This is main driver program of the query processor
*/
package qp;

import java.io.*;

import qp.utils.*;
import qp.operators.*;
import qp.optimizer.*;
import qp.parser.*;

public class QueryMain {

    private static PrintWriter out;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("usage: java QueryMain <queryfilename> <resultfile>");
            System.exit(1);
        }

        /** Enter the number of bytes per page **/
        System.out.println("enter the number of bytes per page");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String temp;
        try {
            temp = in.readLine();
            int pageSize = Integer.parseInt(temp);
            Batch.setPageSize(pageSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String queryFile = args[0];
        String resultFile = args[1];
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryFile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryFile);
            System.exit(1);
        }

        // Lex the query
        Scanner sc = new Scanner(source);
        parser p = new parser();
        p.setScanner(sc);

        // Parse the query
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occured while parsing");
            System.exit(1);
        }

        // SQLQuery is the result of the parsing
        SQLQuery sqlquery = p.getSQLQuery();
        int numJoin = sqlquery.getNumJoin();

        /*
         If there are joins then assigns buffers to each join operator
         while preparing the plan

         As buffer manager is not implemented, just input the number of
         buffers available
        */
        if (numJoin != 0) {
            System.out.println("enter the number of buffers available");
            try {
                temp = in.readLine();
                int numBuff = Integer.parseInt(temp);
                BufferManager bm = new BufferManager(numBuff, numJoin);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Check the number of buffers available is enough or not **/
        int numBuff = BufferManager.getBuffersPerJoin();
        if (numJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per a join operator ");
            System.exit(1);
        }

        /*
        This part is used When some random initial plan is required instead of complex optimized plan

         RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
         Operator logicalRoot = rip.prepareInitialPlan();
         PlanCost pc = new PlanCost();
         int initCost = pc.getCost(logicalRoot);
         Debug.PPrint(logicalRoot);
         System.out.print("   "+initCost);
         System.out.println();
        */

        /*
         Use random Optimization algorithm to get a random optimized
         execution plan
         */
        RandomOptimizer ro = new RandomOptimizer(sqlquery);
        Operator logicalRoot = ro.getOptimizedPlan();
        if (logicalRoot == null) {
            System.out.println("root is null");
            System.exit(1);
        }

        /* Preparing the execution plan */
        Operator root = RandomOptimizer.makeExecPlan(logicalRoot);

        /* Print final Plan */
        System.out.println("----------------------Execution Plan----------------");
        Debug.PPrint(root);
        System.out.println();


        /* Ask user whether to continue execution of the program **/

        System.out.println("enter 1 to continue, 0 to abort ");


        try {
            temp = in.readLine();
            int flag = Integer.parseInt(temp);
            if (flag == 0) {
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        long startTime = System.currentTimeMillis();


        if (!root.open()) {
            System.out.println("Root: Error in opening of root");
            System.exit(1);
        }
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultFile)));
        } catch (IOException io) {
            System.out.println("QueryMain:error in opening result file: " + resultFile);
            System.exit(1);
        }

        /* Print the schema of the result */
        Schema schema = root.getSchema();
        Debug.PPrint(schema);
        Batch resultBatch;

        /* Print each tuple in the result */
        while ((resultBatch = root.next()) != null) {
            for (int i = 0; i < resultBatch.size(); i++) {
                Debug.PPrint(resultBatch.elementAt(i));
            }
        }
        root.close();
        out.close();

        long endTime = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;
        System.out.println("Execution time = " + executionTime);
    }
}







