package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by michaellimantara on 21/3/17.
 */
public class SortMergeJoin extends Join {

    private static boolean CLEANUP_FILES = true;

    private ExternalSort leftSort;
    private ExternalSort rightSort;

    private int leftJoinAttrIdx;
    private int rightJoinAttrIdx;

    private int batchSize;

    private List<File> leftFiles;
    private List<File> rightFiles;

    private int leftBufferIdx = -1;
    private Batch leftBuffer;

    private int rightBufferOffset = 0;
    private List<Batch> rightBuffer = new LinkedList<>();

    private Batch rightRunningBuffer;
    private int rightRunningBufferIdx = -1;

    private int rightBufferSize;

    private int leftTupleIdx;
    private int rightTupleIdx;
    private int rightFirstMatchIdx;
    private boolean hasMatch;


    public SortMergeJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    @Override
    public boolean open() {
        try {
            List<Order> leftSortOrders = Arrays.asList(new Order(getCondition().getLhs(), Order.OrderType.ASC));
            List<Order> rightSortOrders = Arrays.asList(new Order((Attribute) getCondition().getRhs(), Order.OrderType.ASC));

            // Find the batch size
            int tupleSize = getSchema().getTupleSize();
            batchSize = Batch.getPageSize() / tupleSize;

            // Find the index of join attribute of in each relation
            leftJoinAttrIdx = getLeft().getSchema().indexOf(getCondition().getLhs());
            rightJoinAttrIdx = getRight().getSchema().indexOf((Attribute) getCondition().getRhs());

            leftTupleIdx = 0;
            rightTupleIdx = 0;
            rightFirstMatchIdx = 0;
            hasMatch = false;

            // Sort the 2 relations
            leftSort = new ExternalSort(left, leftSortOrders, numBuff);
            rightSort = new ExternalSort(right, rightSortOrders, numBuff);

            if (!(leftSort.open() && rightSort.open())) {
                return false;
            }

            leftFiles = writeOperatorToFile(leftSort, "SMJ-Left");
            rightFiles = writeOperatorToFile(rightSort, "SMJ-Right");

            leftSort.close();
            rightSort.close();

            rightBufferSize = getNumBuff() - 3;  // reserve 1 output buf, 1 for left input, 1 for "running" right input

            initializeRightBuffer();

            return true;
        } catch (IOException|ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public Batch next() {
        try {
            return nextThrows();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Produces 1 batch of output
     */
    private Batch nextThrows() throws IOException, ClassNotFoundException {
        Batch joinResult = new Batch(batchSize);

        while (true) {
            if (joinResult.isFull() || hasExhaustedLeftTuples() || hasExhaustedRightTuples()) {
                break;
            }
            Tuple leftTuple = readLeftTupleAtIndex(leftTupleIdx);
            Tuple rightTuple = readRightTupleAtIndex(rightTupleIdx);
            int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftJoinAttrIdx, rightJoinAttrIdx);
            if (comparison < 0) {           // if left tuple < right tuple on join attribute,
                leftTupleIdx++;      // we move to the next left tuple
                if (hasMatch) {
                    rightTupleIdx = rightFirstMatchIdx;
                }
                hasMatch = false;
            } else if (comparison > 0) {    // if left tuple > right tuple on join attribute,
                rightTupleIdx++;     // we move to the next right tuple
                hasMatch = false;
            } else {  // if left tuple = right tuple on join attribute, we start joining the tuples
                if (!hasMatch) {
                    rightFirstMatchIdx = rightTupleIdx;
                    hasMatch = true;
                }
                Tuple joinTuple = leftTuple.joinWith(rightTuple);
                joinResult.add(joinTuple);
                rightTupleIdx++;
            }
        }
        return joinResult.isEmpty() ? null : joinResult;  // return null to signify end of result
    }

    private Tuple readLeftTupleAtIndex(int idx) throws IOException, ClassNotFoundException, IndexOutOfBoundsException {
        int tupleSize = getLeft().getSchema().getTupleSize();
        int batchSize = Batch.getPageSize() / tupleSize;

        int batchIndex = idx / batchSize;
        int tupleIdxInBatch = idx % batchSize;

        Batch batch = readLeftBatch(batchIndex);
        return batch.elementAt(tupleIdxInBatch);
    }

    private Tuple readRightTupleAtIndex(int idx) throws IOException, ClassNotFoundException {
        int tupleSize = getRight().getSchema().getTupleSize();
        int batchSize = Batch.getPageSize() / tupleSize;

        int batchIndex = idx / batchSize;
        int tupleIdxInBatch = idx % batchSize;

        Batch batch = readRightBatch(batchIndex);
        return batch.elementAt(tupleIdxInBatch);
    }

    private boolean hasExhaustedLeftTuples() {
        try {
            readLeftTupleAtIndex(leftTupleIdx);
            return false;
        } catch (IndexOutOfBoundsException e) {
            return true;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasExhaustedRightTuples() {
        try {
            readRightTupleAtIndex(rightTupleIdx);
            return false;
        } catch (IndexOutOfBoundsException e) {
            return true;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean close() {
        rightBuffer.clear();
        leftBuffer.clear();

        if (CLEANUP_FILES) {
            for (File file: leftFiles) {
                file.delete();
            }

            for (File file: rightFiles) {
                file.delete();
            }
        }

        return super.close();
    }

    private void initializeRightBuffer() throws IOException, ClassNotFoundException {
        rightBufferOffset = 0;
        rightBuffer.clear();
        for (int i = 0; i < rightBufferSize; i++) {
            if (i >= rightFiles.size()) {
                break;
            }

            Batch batch = readBatchFromFile(rightFiles.get(i));
            rightBuffer.add(batch);
        }
    }

    private Batch readLeftBatch(int idx) throws IOException, ClassNotFoundException, IndexOutOfBoundsException {
        if (idx == leftBufferIdx) {
            return leftBuffer;
        }
        File file = leftFiles.get(idx);
        leftBuffer = readBatchFromFile(file);
        leftBufferIdx = idx;
        return leftBuffer;
    }

    private Batch readRightBatch(int idx) throws IOException, ClassNotFoundException {
        if (isInRightBuffer(idx)) {
            return readFromBuffer(idx);
        }
        if (idx < rightBufferOffset || rightBufferSize == 0) {
            return readToRunningBuffer(idx);
        }
        while (!isInRightBuffer(idx)) {  // must be beyond the current buffer scope
            advanceBuffer();             // hence we advance the buffer
        }
        return readFromBuffer(idx);
    }

    private void advanceBuffer() throws IOException, ClassNotFoundException {
        int nextRightBatchToRead = rightBufferOffset + rightBufferSize;
        rightBuffer.remove(0);
        Batch batch = readBatchFromFile(rightFiles.get(nextRightBatchToRead));
        rightBuffer.add(batch);
        rightBufferOffset++;
    }

    private Batch readToRunningBuffer(int idx) throws IOException, ClassNotFoundException {
        if (rightRunningBufferIdx == idx) {
            return rightRunningBuffer;
        }
        rightRunningBuffer = readBatchFromFile(rightFiles.get(idx));
        rightRunningBufferIdx = idx;
        return rightRunningBuffer;
    }

    private Batch readFromBuffer(int idx) {
        return rightBuffer.get(idx - rightBufferOffset);
    }

    private boolean isInRightBuffer(int idx) {
        return (rightBufferOffset <= idx) && (idx < rightBufferOffset + rightBufferSize);
    }

    private List<File> writeOperatorToFile(Operator operator, String prefix) throws IOException {
        Batch batch;
        int count = 0;
        List<File> files = new ArrayList<>();
        while ((batch = operator.next()) != null) {
            File file = new File(prefix + "-" + count);
            count += 1;
            writeBatchToFile(batch, file);
            files.add(file);
        }
        return files;
    }

    private void writeBatchToFile(Batch batch, File file) throws IOException {
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
        objectOutputStream.writeObject(batch);

    }

    private Batch readBatchFromFile(File file) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
        return (Batch) objectInputStream.readObject();
    }


}
