package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;

public class BlockNestedJoin extends Join {

    private int batchSize; // Number of tuple per batch

    private int leftIndex, rightIndex;

    private String tempFileName;

    private Batch leftBatch;
    private Batch rightBatch;
    private Queue<Batch> leftBatches;

    private ObjectInputStream in;

    private int leftCursor;
    private int rightCursor;

    private boolean leftEndReached;
    private boolean rightEndReached;

    private static int fileId = 0; // ID of generated file

    public BlockNestedJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
        this.schema = join.getSchema();
        this.jointype = join.getJoinType();
        this.numBuff = join.getNumBuff();
    }

    public boolean open() {
        this.setBatchSize();
        this.setIndexFromJoinAttribute();
        this.resetCursors();
        return this.materializeRightTable() && this.left.open();
    }

    public Batch next() {
        if (this.leftEndReached && this.leftBatches.isEmpty()) {
            this.close();
            return null;
        }

        Batch outBatch = new Batch(this.batchSize);

        while (!outBatch.isFull()) {

            // End buffer reached, cycle the left buffer
            if (this.leftCursor == 0 && this.rightEndReached) {
                this.selectNextLeftBuffer();
                if (this.leftBatch == null) {
                    this.leftEndReached = true;
                    return outBatch;
                }
            }

            // Iterate right buffer until end
            while (!this.rightEndReached) {
                try {
                    if (this.leftCursor == 0 && this.rightCursor == 0) {
                        this.rightBatch = (Batch) this.in.readObject();
                    }

                    for (int i = this.leftCursor; i < this.leftBatch.size(); i++) {
                        for (int j = this.rightCursor; j < this.rightBatch.size(); j++) {
                            Tuple leftTuple = leftBatch.elementAt(i);
                            Tuple rightTuple = rightBatch.elementAt(j);

                            if (leftTuple.checkJoin(rightTuple, this.leftIndex, this.rightIndex)) {
                                Tuple result = leftTuple.joinWith(rightTuple);
                                outBatch.add(result);

                                if (outBatch.isFull()) {
                                    if (i == leftBatch.size() - 1 && j == rightBatch.size() - 1) {
                                        this.leftCursor = 0;
                                        this.rightCursor = 0;
                                    } else if (i != leftBatch.size() - 1 && j == rightBatch.size() - 1) {
                                        this.leftCursor = i + 1;
                                        this.rightCursor = 0;
                                    } else if (i == leftBatch.size() - 1 && j != rightBatch.size() - 1) {
                                        this.leftCursor = i;
                                        this.rightCursor = j + 1;
                                    } else {
                                        this.leftCursor = i;
                                        this.rightCursor = j + 1;
                                    }
                                }
                                return outBatch;
                            }
                        }
                        this.rightCursor = 0;
                    }
                    this.leftCursor = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException ee) {
                        System.err.println("BlockNestedJoin: Temp File Reading Error");
                    }
                    this.rightEndReached = true;
                } catch (ClassNotFoundException c) {
                    System.err.println("BlockNestedJoin: Deserialization Error");
                    System.exit(1);
                } catch (IOException e) {
                    System.err.println("BlockNestedJoin: Temp File Reading Error");
                    System.exit(1);
                }
            }
        }
        return outBatch;
    }

    public boolean close() {
        File f = new File(this.tempFileName);
        return f.delete();
    }

    private void resetCursors() {
        this.leftCursor = 0;
        this.rightCursor = 0;
        this.leftEndReached = false;
        this.rightEndReached = true;
    }

    private void setBatchSize() {
        int tupleSize = this.schema.getTupleSize();
        this.batchSize = Batch.getPageSize() / tupleSize;
        this.leftBatches = new LinkedList<>();
    }

    private boolean materializeRightTable() {
        if (!this.right.open()) {
            return false;
        } else {
            this.tempFileName = this.getUniqueFileName();

            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(this.tempFileName));
                Batch r;
                while ((r = right.next()) != null) {
                    out.writeObject(r);
                }
                out.close();
            } catch (IOException e) {
                System.out.println("BlockNestedJoin: Error in writing the temporary file");
                return false;
            }

            return right.close();
        }
    }

    private void setIndexFromJoinAttribute() {
        Attribute leftAtrribute = this.con.getLhs();
        Attribute rightAttribute = (Attribute) this.con.getRhs();

        this.leftIndex = this.left.getSchema().indexOf(leftAtrribute);
        this.rightIndex = this.right.getSchema().indexOf(rightAttribute);
    }

    private String getUniqueFileName() {
        int id = BlockNestedJoin.fileId;
        BlockNestedJoin.fileId++;
        return "BNJTemp-" + id;
    }

    private void selectNextLeftBuffer() {
        if (this.leftBatches.isEmpty()) {
            // Load new batches to left buffers
            for (int i = 1; i <= this.numBuff - 2; i++) {
                Batch b = left.next();
                if (b != null) {
                    this.leftBatches.offer(b);
                } else {
                    this.leftEndReached = true;
                    break;
                }
            }
        }
        if (!this.leftBatches.isEmpty()) {
            this.leftBatch = this.leftBatches.poll();
            // Reset right materialized stream
            try {
                this.in = new ObjectInputStream(new FileInputStream(this.tempFileName));
                this.rightEndReached = false;
            } catch (IOException e) {
                System.err.println("BlockNestedJoin: Error in reading the file");
                System.exit(1);
            }
        } else {
            this.leftBatch = null;
        }
    }
}
