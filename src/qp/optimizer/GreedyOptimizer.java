package qp.optimizer;

import qp.operators.*;
import qp.utils.Condition;
import qp.utils.SQLQuery;
import qp.utils.Schema;
import qp.utils.Attribute;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GreedyOptimizer {

    private final SQLQuery query;

    private class RelationSchemaPair implements Comparable<RelationSchemaPair> {
        private final String name;
        private Operator operator;
        private int nTuple;

        RelationSchemaPair(String name) {
            Schema schema = fetchSchema(name);
            this.name = name;
            this.operator = new Scan(this.name, OpType.SCAN);
            this.operator.setSchema(schema);
            PlanCost pc = new PlanCost();
            this.nTuple = pc.getStatistics((Scan) this.operator);
        }

        Operator getOperator() {
            return this.operator;
        }

        Schema getSchema() {
            return this.operator.getSchema();
        }

        int countTuple() {
            return this.nTuple;
        }

        String getName() {
            return this.name;
        }

        void setOperator(Operator operator) {
            this.operator = operator;
        }

        @Override
        public int compareTo(RelationSchemaPair other) {
            int left = this.countTuple();
            int right = other.countTuple();
            return Integer.compare(left, right);
        }

        @Override
        public int hashCode() {
            return this.name.hashCode();
        }

        private Schema fetchSchema(String relation) {
            try {
                ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(relation + ".md"));
                return (Schema) ois.readObject();
            } catch (Exception e) {
                throw new RuntimeException("GreedyOptimizer: Error reading table schema");
            }
        }
    }

    public GreedyOptimizer(SQLQuery query) {
        this.query = query;
    }

    public Operator getOptimizedPlan() {

        // Collect Relations from Joins
        Map<String, RelationSchemaPair> relations =
                Arrays.stream(this.query.getFromList().toArray())
                    .map(Object::toString)
                    .map(RelationSchemaPair::new)
                    .collect(Collectors.toMap(
                        RelationSchemaPair::getName,
                        Function.identity()
                    ));

        // Collect and assign Selection operators
        Arrays.stream(this.query.getSelectionList().toArray())
            .map(o -> (Condition) o)
            .filter(condition -> condition.getOpType() == Condition.SELECT)
            .forEach(condition -> {
                String name = condition.getLhs().getTabName();
                RelationSchemaPair rsp = relations.get(name);
                Select select = new Select(
                    rsp.getOperator(),
                    condition,
                    OpType.SELECT
                );
                select.setSchema(select.getSchema());
                rsp.setOperator(select);
            });

        Operator joinRoot = null;

        // No Join
        if (this.query.getJoinList().isEmpty()) {
            joinRoot = relations.get(this.query.getFromList().firstElement().toString()).getOperator();
        } else {
            // Create Left-Deep join tree sorted by tuple size.
            Map<String, List<Condition>> joinMap = new HashMap<>();

            // Initialise join list
            Arrays.stream(this.query.getJoinList().toArray())
                .map((o) -> (Condition) o)
                .forEach(cond -> {
                    String leftName = cond.getLhs().getTabName();
                    joinMap.putIfAbsent(leftName, new ArrayList<>());
                    joinMap.get(leftName).add(cond);
                });

            List<RelationSchemaPair> sortedRelations = joinMap.keySet()
                .stream()
                .map(relations::get)
                .sorted(Comparator.comparingInt(RelationSchemaPair::countTuple))
                .collect(Collectors.toList());

            int joinNum = 0;

            for (RelationSchemaPair relation: sortedRelations) {
                List<Condition> conditions = new ArrayList<>();

                joinMap.values().forEach(cs -> {
                    cs.forEach(condition -> {
                        String left = condition.getLhs().getTabName();
                        String right = ((Attribute) condition.getRhs()).getTabName();
                        if (left.equals(relation.getName()) || right.equals(relation.getName())) {
                            conditions.add(condition);
                        }
                    });
                });

                conditions.forEach(condition ->
                    joinMap.values().forEach(cs -> cs.remove(condition))
                );

                conditions.sort((c1, c2) -> {
                    String c1n = ((Attribute) c1.getRhs()).getTabName();
                    String c2n = ((Attribute) c2.getRhs()).getTabName();
                    return Integer.compare(
                            relations.get(c1n).countTuple(),
                            relations.get(c2n).countTuple());
                });

                for (Condition condition : conditions) {
                    RelationSchemaPair right = relations.get(((Attribute) condition.getRhs()).getTabName());
                    RelationSchemaPair left = relations.get(condition.getLhs().getTabName());
                    if (joinRoot == null) {
                        joinRoot = relation.getOperator();
                    }
                    Join join;
                    Set<Attribute> intersect = new HashSet<>(joinRoot.getSchema().getAttList());
                    Set<Attribute> leftAttrs = new HashSet<>(left.getSchema().getAttList());
                    intersect.retainAll(leftAttrs);
                    if (!intersect.isEmpty()) {
                        join = new Join(
                            joinRoot,
                            right.getOperator(),
                            condition,
                            OpType.JOIN
                        );
                        join.setSchema(joinRoot.getSchema().joinWith(right.getSchema()));
                    } else {
                        condition.flip();
                        join = new Join(
                            joinRoot,
                            left.getOperator(),
                            condition,
                            OpType.JOIN
                        );
                        join.setSchema(joinRoot.getSchema().joinWith(left.getSchema()));
                    }
                    join.setNodeIndex(joinNum);
                    joinNum++;
                    join.setJoinType(JoinType.BLOCKNESTED);
                    joinRoot = join;
                }
            }
        }

        Vector projectList = this.query.getProjectList();
        if (projectList.isEmpty()) {
            return joinRoot;
        } else {
            Project project = new Project(
                joinRoot,
                this.query.getProjectList(),
                OpType.PROJECT
            );
            Schema schema = joinRoot.getSchema().subSchema(projectList);
            project.setSchema(schema);
            return project;
        }
    }
}
