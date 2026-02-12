import os
import sys
import psutil
import time
import logging
from collections import defaultdict

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from api.network_builder import get_db_connection, build_graph

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_memory(stage):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / 1024 / 1024
    logger.info(f"[MEMORY] {stage}: {mem:.2f} MB")

def test_graph_build():
    log_memory("Start")
    
    conn = get_db_connection()
    try:
        log_memory("DB Connection")
        
        # Run build_graph
        graph, node_to_int, int_to_node = build_graph(conn)
        
        log_memory("Graph Built")
        
        logger.info(f"Graph Nodes: {len(graph)}")
        logger.info(f"Unique Entities: {len(int_to_node)}")
        
        # Check size of first few nodes
        first_keys = list(graph.keys())[:5]
        for k in first_keys:
            logger.info(f"Node {k} edges: {len(graph[k])} (Type: {type(graph[k])})")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        conn.close()
        log_memory("End")

if __name__ == "__main__":
    test_graph_build()
