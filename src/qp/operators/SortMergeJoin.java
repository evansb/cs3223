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

    private ExternalSort leftSort;
    private ExternalSort rightSort;

    private Batch curLeftBatch;

    private List<File> leftFiles;
    private List<File> rightFiles;
    private int rightBufferOffset = 0;
    private List<Batch> rightBuffer = new LinkedList<>();
    private Batch rightRunningBuffer;
    private int rightRunningBufferIdx = -1;

    private int rightBufferSize;


    public SortMergeJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
    }

    @Override
    public boolean open() {
        try {
            List<Order> leftSortOrders = Arrays.asList(new Order(getCondition().getLhs(), Order.OrderType.ASC));
            List<Order> rightSortOrders = Arrays.asList(new Order((Attribute) getCondition().getRhs(), Order.OrderType.ASC));

            leftSort = new ExternalSort(left, leftSortOrders, numBuff);
            rightSort = new ExternalSort(right, rightSortOrders, numBuff);

            if (!(leftSort.open() && rightSort.open())) {
                return false;
            }

            leftFiles = writeOperatorToFile(leftSort, "SMJ-Left");
            rightFiles = writeOperatorToFile(rightSort, "SMJ-Right");

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

    private Batch nextThrows() throws IOException, ClassNotFoundException {
        for (File leftFile: leftFiles) {
            Batch leftBatch = readBatchFromFile(leftFile);
            for (int i = 0; i < leftBatch.size(); i++) {
                Tuple tuple = leftBatch.elementAt(i);

            }
        }
    }

    private void initializeRightBuffer() throws IOException, ClassNotFoundException {
        rightBufferOffset = 0;
        rightBuffer.clear();
        for (int i = 0; i < rightBufferSize; i++) {
            Batch batch = readBatchFromFile(rightFiles.get(i));
            rightBuffer.add(batch);
        }
    }

    private Batch readRightBatch(int idx) throws IOException, ClassNotFoundException {
        if (isInRightBuffer(idx)) {
            return readFromBuffer(idx);
        }
        if (idx < rightBufferOffset) {
            return readToRunningBuffer(idx);
        }
        while (!isInRightBuffer(idx)) {  // must be beyond the current buffer scope
            advanceBuffer();  // hence we advance the buffer
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
        Batch batch = readBatchFromFile(rightFiles.get(idx));
        rightRunningBufferIdx = idx;
        return batch;
    }

    private Batch readFromBuffer(int idx) {
        return rightBatchBuffer.get(idx - rightBufferOffset);
    }

    private boolean isInRightBuffer(int idx) {
        return (rightBufferOffset <= idx) && (idx < rightBufferOffset + rightBufferSize);
    }


    @Override
    public boolean close() {
        return super.close();
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
