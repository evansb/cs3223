package qp.utils;

import java.io.*;

/**
 * Created by andhieka on 27/3/17.
 */
public class OOSPrettyPrint {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        File workingDir = new File(".");
        for (File file: workingDir.listFiles()) {
            if (file.getName().startsWith("EStemp-") && !file.getName().endsWith("pretty")) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
                PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file.getName() + "-pretty")));
                try {
                    while (true) {
                        Batch batch = (Batch) ois.readObject();
                        out.println("[");
                        for (Object tupleObject: batch.tuples) {
                            Tuple t = (Tuple) tupleObject;
                            for (Object datum: t.data()) {
                                out.print(datum);
                                out.print("\t");
                            }
                            out.println();
                        }
                        out.println("]");
                    }
                } catch (EOFException e) {
                }
                out.close();
                ois.close();
            }
        }
    }
}
