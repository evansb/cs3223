package qp.operators;

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

    private ArrayList<Batch> sortedDataInBatch;
    private int outputBatchIndex;

    private int fileNum;
    private List<File> sortedRuns;

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
        sortedRuns = new ArrayList<>();
        comparator = composeComparator();

        // Phase 1
        generateSortedRuns();

        // Phase 2
        executeMerge();

        return true;
    }

    public Batch next() {
        if (outputBatchIndex >= sortedDataInBatch.size()) {
            return null;
        }

        return sortedDataInBatch.get(outputBatchIndex++);
    }

    public boolean close() {
        clearSortedRuns(sortedRuns);
        return super.close();
    }


    // External Sorting implementation


    private void generateSortedRuns() {
        Batch currentBatch = source.next();
        while (currentBatch != null) {
            ArrayList<Batch> run = new ArrayList<>();
            for (int i = 0; i < numBuffers; i++) {
                run.add(currentBatch);
                currentBatch = source.next();
                if (currentBatch == null) {
                    break;
                }
            }

            sortRun(run);
            File sortedRun = writeRun(run);
            sortedRuns.add(sortedRun);
        }
    }

    private void executeMerge() {
        int numBuffersAvailable = numBuffers - 1;

        while (sortedRuns.size() > 0) {
            int numberOfSortedRuns = sortedRuns.size();
            List<File> newSortedRuns = new ArrayList<>();
            for (int subRound = 0; subRound * numBuffersAvailable < numberOfSortedRuns; subRound++) {
                int startIdx = subRound * numBuffersAvailable;
                int endIdx = (subRound + 1) * numBuffersAvailable;
                endIdx = Math.min(endIdx, sortedRuns.size());  // in case of last few runs

                List<File> runsToSort = sortedRuns.subList(startIdx, endIdx);
                File resultSortedRun = mergeSortedRuns(runsToSort);
                newSortedRuns.add(resultSortedRun);
            }

            // Replace sorted runs with the newer batch
            clearSortedRuns(sortedRuns);
            sortedRuns = newSortedRuns;
        }
    }

    private void clearSortedRuns(List<File> sortedRuns) {
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
        Batch outputBuffer = new Batch(inputBuffers.get(0).capacity());
        File outputFile = null;
        int[] batchPointers = new int[numBuffersAvailable];

        while (true) {
            Tuple smallest = null;
            for (int i = 0; i < inputBuffers.size(); i++) {
                Batch batch = inputBuffers.get(i);
                Tuple tuple = batch.elementAt(batchPointers[i]);
                if (smallest == null || comparator.compare(tuple, smallest) < 0) {
                    smallest = tuple;
                }
            }
            if (smallest == null) {
                break;
            }
            outputBuffer.add(smallest);

            if (outputBuffer.isFull()) {
                if (outputFile == null) {
                    outputFile = writeRun(Arrays.asList(outputBuffer));
                } else {
                    appendRun(outputBuffer, outputFile);
                }

            }
        }

        return outputFile;
    }

    private void sortRun(ArrayList<Batch> run) {
        List<Tuple> tuples = new ArrayList<>();
        for (Batch batch: run) {
            addBatch(batch, tuples);
        }
        Collections.sort(tuples, comparator);
    }

    private void addBatch(Batch batch, List<Tuple> tuples) {
        for (int i = 0; i < batch.size(); i++) {
            tuples.add(batch.elementAt(i));
        }
    }

    private File writeRun(List<Batch> run) {
        try {
            File temp = new File("EStemp-" + fileNum);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(temp));
            for (Batch batch: run) {
                out.writeObject(batch);
            }
            fileNum++;
            out.close();

            return temp;
        } catch (IOException e) {
            System.out.println("ExternalSort: Error in writing the temporary file");
        }
        return null;
    }

    private void appendRun(Batch run, File destination) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(destination, true));
            out.writeObject(run);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Batch readBatch(ObjectInputStream inputStream) {
        try {
            Batch batch = (Batch) inputStream.readObject();
            return batch;
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
