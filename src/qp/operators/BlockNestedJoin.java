package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

/**
 * The BlockNested join uses (B-2) batches for the left table, 1 batch for right
 * table, and 1 batch for output.
 */
public class BlockNestedJoin extends Join {

    private int batchSize; // Number of tuple per batch

    private int leftIndex, rightIndex; // Index of the join column in tuple

    private String tempFileName; // Temporary file for right table.

    private Batch rightBatch; // Right batch

    private List<Batch> leftBatches = new LinkedList<>(); // Left batches
    private ArrayList<Tuple> leftTuples = new ArrayList<>(); // Flattened left batches

    private ObjectInputStream in;

    // Variables used during the iteration
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
                this.loadLeftBatches();
                if (this.leftBatches.isEmpty()) {
                    this.leftEndReached = true;
                    return outBatch;
                }
            }

            // Iterate right buffer until end
            while (!this.rightEndReached) {
                try {
                    // If we are in the beginning of left batches, read
                    // a batch from materialized file.
                    if (this.leftCursor == 0 && this.rightCursor == 0) {
                        this.rightBatch = (Batch) this.in.readObject();
                    }

                    for (int i = this.leftCursor; i < this.leftTuples.size(); i++) {
                        for (int j = this.rightCursor; j < this.rightBatch.size(); j++) {
                            Tuple leftTuple = leftTuples.get(i);
                            Tuple rightTuple = rightBatch.elementAt(j);

                            if (leftTuple.checkJoin(rightTuple, this.leftIndex, this.rightIndex)) {
                                Tuple result = leftTuple.joinWith(rightTuple);
                                outBatch.add(result);

                                if (outBatch.isFull()) {
                                    // Left batches and right batch done
                                    if (i == leftTuples.size() - 1 && j == rightBatch.size() - 1) {
                                        this.leftCursor = 0;
                                        this.rightCursor = 0;

                                        // Right batch done
                                    } else if (i != leftTuples.size() - 1 && j == rightBatch.size() - 1) {
                                        this.leftCursor = i + 1;
                                        this.rightCursor = 0;

                                        //  Next tuple in right batch
                                    } else if (i == leftTuples.size() - 1 && j != rightBatch.size() - 1) {
                                        this.leftCursor = i;
                                        this.rightCursor = j + 1;
                                    } else {
                                        this.leftCursor = i;
                                        this.rightCursor = j + 1;
                                    }
                                    return outBatch;
                                }
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

    private void loadLeftBatches() {
        // Load new batches to left buffers
        this.leftBatches.clear();
        this.leftTuples.clear();
        for (int i = 1; i <= this.numBuff - 2; i++) {
            Batch b = left.next();
            if (b != null) {
                this.leftBatches.add(b);
            }
        }
        for (Batch b: this.leftBatches) {
            for (int i = 0; i < b.size(); i++) {
                this.leftTuples.add(b.elementAt(i));
            }
        }
        if (!this.leftBatches.isEmpty()) {
            // Reset right materialized stream
            try {
                FileInputStream file = new FileInputStream(this.tempFileName);
                InputStream buffer = new BufferedInputStream(file);
                this.in = new ObjectInputStream(buffer);
                this.rightEndReached = false;
            } catch (IOException e) {
                System.err.println("BlockNestedJoin: Error in reading the file");
                System.exit(1);
            }
        }
    }
}
