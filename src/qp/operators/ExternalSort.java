package qp.operators;

import qp.utils.AppendingObjectOutputStream;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

/**
 * Created by michaellimantara on 20/3/17.
 */
public class ExternalSort extends Operator {

    private Operator source;
    private int numBuffers;
    private List<Order> sortOrders;
    private Comparator<Tuple> comparator;

    private int fileNum;
    private int roundNum;
    private List<File> sortedRunFiles;

    private ObjectInputStream iteratorInputStream;

    private int initialNumTuples;
    private int tuplesProcessedThisRound;
    private int tupleSize;
    private int batchSize;

    private static boolean FILE_CLEANUP = false;



    public ExternalSort(Operator source, List<Order> sortOrders, int numBuffers) {
        super(OpType.SORT);
        this.source = source;
        this.sortOrders = sortOrders;
        this.numBuffers = numBuffers;
    }

    public boolean open() {
        if (!source.open()) {
            return false;
        }

        // Initialization
        fileNum = 0;
        roundNum = 0;
        sortedRunFiles = new ArrayList<>();
        comparator = composeComparator();
        tupleSize = source.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Phase 1
        generateSortedRuns();
        roundNum++;
        fileNum = 0;
        System.out.printf("Initial number of tuples = %d\n", initialNumTuples);

        // Phase 2
        executeMerge();

        return true;
    }

    public Batch next() {
        assert sortedRunFiles.size() == 1;
        try {
            if (iteratorInputStream == null) {
                iteratorInputStream = new ObjectInputStream(new FileInputStream(sortedRunFiles.get(0)));
            }

            return readBatch(iteratorInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean close() {
        clearSortedRuns(sortedRunFiles);
        try {
            iteratorInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return super.close();
    }


    // External Sorting implementation


    private void generateSortedRuns() {
        initialNumTuples = 0;
        Batch currentBatch = source.next();  // read first batch
        while (currentBatch != null) {
            ArrayList<Batch> run = new ArrayList<>();
            for (int i = 0; i < numBuffers; i++) {
                initialNumTuples += currentBatch.size();
                run.add(currentBatch);

                // read next batch
                currentBatch = source.next();
                
                if (currentBatch == null) {
                    break;
                }
            }

            List<Batch> sortedRun = sortedRun(run);
            File sortedRunFile = writeRun(sortedRun);
            sortedRunFiles.add(sortedRunFile);
        }
    }

    private void executeMerge() {
        int numBuffersAvailable = numBuffers - 1;

        while (sortedRunFiles.size() > 1) {
            System.out.printf("ROUND %d. Number of sorted runs = %d.\n", roundNum, sortedRunFiles.size());
            int numberOfSortedRuns = sortedRunFiles.size();
            List<File> newSortedRuns = new ArrayList<>();
            tuplesProcessedThisRound = 0;
            for (int subRound = 0; subRound * numBuffersAvailable < numberOfSortedRuns; subRound++) {
                int startIdx = subRound * numBuffersAvailable;
                int endIdx = (subRound + 1) * numBuffersAvailable;
                endIdx = Math.min(endIdx, sortedRunFiles.size());  // in case of last few runs

                List<File> runsToSort = sortedRunFiles.subList(startIdx, endIdx);
                File resultSortedRun = mergeSortedRuns(runsToSort);
                newSortedRuns.add(resultSortedRun);
            }

            roundNum++;
            fileNum = 0;
            System.out.printf("\tTuples processed = %d\n", tuplesProcessedThisRound);
            assert initialNumTuples == tuplesProcessedThisRound;

            // Replace sorted runs with the newer batch
            clearSortedRuns(sortedRunFiles);
            sortedRunFiles = newSortedRuns;
        }
    }

    private void clearSortedRuns(List<File> sortedRuns) {
        if (!FILE_CLEANUP) return;

        for (File run : sortedRuns) {
            run.delete();
        }
    }

    /**
     * Receives a list of sorted runs and produces one longer sorted run.
     */
    private File mergeSortedRuns(List<File> sortedRuns) {
        assert sortedRuns.size() <= numBuffers - 1;

        if (sortedRuns.isEmpty()) {
            return null;
        }

        int numBuffersAvailable = sortedRuns.size();
        ArrayList<Batch> inputBuffers = new ArrayList<>();

        List<ObjectInputStream> inputStreams = new ArrayList<>();

        // open files
        for (File sortedRun: sortedRuns) {
            try {
                ObjectInputStream is = new ObjectInputStream(new FileInputStream(sortedRun));
                inputStreams.add(is);
            } catch (IOException e) {
                System.out.println("ExternalSort: Error in reading the temporary sorted runs");
            }
        }

        // do initial reading
        for (ObjectInputStream inputStream: inputStreams) {
            Batch batch = readBatch(inputStream);
            inputBuffers.add(batch);
        }

        // merging
        Batch outputBuffer = new Batch(batchSize);
        File outputFile = null;
        int[] batchPointers = new int[numBuffersAvailable];

        while (true) {
            Tuple smallest = null;
            int indexOfSmallest = 0;
            for (int i = 0; i < inputBuffers.size(); i++) {
                Batch batch = inputBuffers.get(i);
                if (batchPointers[i] >= batch.size()) {
                    continue;
                }

                Tuple tuple = batch.elementAt(batchPointers[i]);
                if (smallest == null || comparator.compare(tuple, smallest) < 0) {
                    smallest = tuple;
                    indexOfSmallest = i;
                }
            }
            if (smallest == null) {
                break;
            }

            batchPointers[indexOfSmallest] += 1;
            if (batchPointers[indexOfSmallest] == inputBuffers.get(indexOfSmallest).capacity()) {
                Batch batch = readBatch(inputStreams.get(indexOfSmallest));
                if (batch != null) {
                    inputBuffers.set(indexOfSmallest, batch);
                    batchPointers[indexOfSmallest] = 0;
                }
            }
            outputBuffer.add(smallest);
            tuplesProcessedThisRound++;

            if (outputBuffer.isFull()) {
                if (outputFile == null) {
                    outputFile = writeRun(Arrays.asList(outputBuffer));
                } else {
                    appendRun(outputBuffer, outputFile);
                }
                outputBuffer.clear();
            }
        }

        if (!outputBuffer.isEmpty()) {
            if (outputFile == null) {
                outputFile = writeRun(Arrays.asList(outputBuffer));
            } else {
                appendRun(outputBuffer, outputFile);
            }
        }

        return outputFile;
    }

    private List<Batch> sortedRun(ArrayList<Batch> run) {
        List<Tuple> tuples = new ArrayList<>();
        for (Batch batch: run) {
            addBatch(batch, tuples);
        }
        Collections.sort(tuples, comparator);
        List<Batch> batches = new ArrayList<>();

        Batch currentBatch = new Batch(batchSize);
        for (Tuple tuple: tuples) {
            currentBatch.add(tuple);
            if (currentBatch.isFull()) {
                batches.add(currentBatch);
                currentBatch = new Batch(batchSize);
            }
        }
        if (!currentBatch.isFull()) {
            batches.add(currentBatch);
        }
        return batches;
    }

    private void addBatch(Batch batch, List<Tuple> tuples) {
        for (int i = 0; i < batch.size(); i++) {
            tuples.add(batch.elementAt(i));
        }
    }

    private File writeRun(List<Batch> run) {
        try {
            int numTuples = 0;
            File temp = new File("EStemp-" + roundNum + "-" + fileNum);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(temp));
            for (Batch batch: run) {
                out.writeObject(batch);
                numTuples += batch.size();
            }
            fileNum++;
            out.close();
            System.out.printf("Initialized file %s with %d batches (%d tuples)\n", temp.getName(), run.size(), numTuples);
            return temp;
        } catch (IOException e) {
            System.out.println("ExternalSort: Error in writing the temporary file");
        }
        return null;
    }

    private void appendRun(Batch run, File destination) {
        try {
            long before = destination.length();
            ObjectOutputStream out = new AppendingObjectOutputStream(new FileOutputStream(destination, true));
            out.writeObject(run);
            long after = destination.length();
            assert before + 100 < after;
            out.close();
            System.out.printf("Append file %s with 1 batches (%d tuples)\n", destination.getName(), run.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Batch readBatch(ObjectInputStream inputStream) {
        try {
            Batch batch = (Batch) inputStream.readObject();
            return batch;
        } catch (EOFException e) {
            return null;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            assert false;
        }
        return null;
    }

    private Comparator<Tuple> composeComparator() {
        return new SortComparator(sortOrders, source.getSchema());
    }

    class SortComparator implements Comparator<Tuple> {

        private Schema schema;
        private List<Order> sortOrders;

        SortComparator(List<Order> sortOrders, Schema schema) {
            this.sortOrders = new ArrayList<>(sortOrders);
            this.schema = schema;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (int i = 0; i < sortOrders.size(); i++) {
                Order order = sortOrders.get(i);
                int attributeIdx = schema.indexOf(order.getAttribute());
                int compareResult = Tuple.compareTuples(t1, t2, attributeIdx);
                if (compareResult != 0) {
                    int multiplier = (order.getOrderType() == Order.OrderType.ASC) ? 1 : -1;
                    return multiplier * compareResult;
                }
            }
            return 0;
        }
    }
}
