package simpledb;

import org.apache.log4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;

public class WaitForGraph {

    final static Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass());

    private HashMap<TransactionId, HashSet<TransactionId>> adjVertices;

    public WaitForGraph() {
        adjVertices = new HashMap<>();
    }

    public void printGraph() {
        String totalOut = "";
        for (Map.Entry<TransactionId, HashSet<TransactionId>> entry : adjVertices.entrySet()) {
            String out = "";
            for (TransactionId end : entry.getValue()) {
                out += end.getId() + ", ";
            }
            out = entry.getKey().getId() + " -> " + out + "; ";
            totalOut += out;
        }

        if (Config.debugTransaction()) {
            logger.debug(totalOut);
        }
    }

    public synchronized void addVertex(TransactionId vertex) {
        adjVertices.putIfAbsent(vertex, new HashSet<>());
    }

    /**
     * @param start
     * @param end
     * @return
     */
    public synchronized void addEdge(TransactionId start, TransactionId end) {
        if (start == null || end == null) {
            return;
        }

        if (start.equals(end)) {
            addVertex(start);
            return;
        }

        addVertex(start);
        addVertex(end);

        adjVertices.get(start).add(end);
    }

    public synchronized boolean containsCycle() {
        printGraph();
        for (TransactionId start : adjVertices.keySet()) {
            ArrayList<TransactionId> visited = new ArrayList<>();
            Stack<TransactionId> stack = new Stack<>();

            stack.push(start);
            visited.add(start);

            while (!stack.empty()) {
                TransactionId vertex = stack.pop();

                for (TransactionId end : adjVertices.get(vertex)) {
                    if (visited.contains(end)) {
                        String l = "";
                        for (TransactionId tid : visited) {
                            l += tid.getId() + "->";
                        }
                        l += end.getId();
                        logger.info("cycle found: " + l);
                        return true;
                    }

                    stack.push(end);
                    visited.add(end);
                }
            }
        }
        return false;
    }

    public synchronized boolean removeVertex(TransactionId transactionId) {
        logger.debug("before remove:");
        printGraph();

        adjVertices.remove(transactionId);
        for (HashSet<TransactionId> transactionIds : adjVertices.values()) {
            transactionIds.remove(transactionId);
        }

        logger.debug("after remove:");
        printGraph();

        return true;
    }
}
