import unittest

def get_network_name(p_counts, p_names):
    # Copy of the logic from api/build_networks.py
    
    primary = "Unknown Network"
    if p_counts:
        # 1. Separate principals into "Corporate" vs "Human" (heuristic)
        def is_corporate(name):
            # Common corporate suffixes/keywords
            keywords = ['LLC', 'INC', 'CORP', 'LTD', 'REALTY', 'MANAGEMENT', 'PROPERTIES', 'GROUP', 'HOLDINGS', 'ASSOCIATES', 'PARTNERS', 'TRUST', 'ESTATE', 'HOUSING', 'APTS', 'APARTMENTS', 'CONDO', 'CONDOMINIUM']
            upper = name.upper()
            # Check suffix or presence of keywords
            parts = upper.split()
            if not parts: return False
            if parts[-1].replace('.', '') in keywords: return True
            for k in keywords:
                if f" {k} " in f" {upper} " or f" {k}," in f" {upper} ": return True
            return False

        human_candidates = []
        corporate_candidates = []
        
        for pid, count in p_counts.items():
            name = p_names.get(pid, "Unknown")
            if is_corporate(name):
                corporate_candidates.append((pid, count, name))
            else:
                human_candidates.append((pid, count, name))
        
        # Sort by count (desc)
        human_candidates.sort(key=lambda x: x[1], reverse=True)
        corporate_candidates.sort(key=lambda x: x[1], reverse=True)
        
        if human_candidates:
            # We have humans!
            # If we have 2 significant humans, combine them.
            # Significant = top 2.
            if len(human_candidates) >= 2:
                p1 = human_candidates[0]
                p2 = human_candidates[1]
                # Let's just always combine top 2 if available to be safe/inclusive for partners.
                primary = f"{p1[2]} & {p2[2]}"
            else:
                primary = human_candidates[0][2]
        elif corporate_candidates:
            # Fallback to top corporate principal
            primary = corporate_candidates[0][2]
        else:
            # Should not happen if p_counts is non-empty
            best_pid = max(p_counts.items(), key=lambda x: x[1])[0]
            primary = p_names.get(best_pid)
            
    return primary

class TestNaming(unittest.TestCase):
    def test_corporate_over_human(self):
        # Case: Corporate has higher count, but Human exists
        p_counts = {1: 100, 2: 50}
        p_names = {1: "GARDEN HILL APTS, LLC", 2: "ZVI HOROWITZ"}
        
        name = get_network_name(p_counts, p_names)
        print(f"Test 1 Result: {name}")
        self.assertEqual(name, "ZVI HOROWITZ")
        
    def test_two_humans(self):
        # Case: Two humans
        p_counts = {1: 100, 2: 90, 3: 10}
        p_names = {1: "ZVI HOROWITZ", 2: "SAMUEL POLLACK", 3: "SOME OTHER GUY"}
        
        name = get_network_name(p_counts, p_names)
        print(f"Test 2 Result: {name}")
        self.assertEqual(name, "ZVI HOROWITZ & SAMUEL POLLACK")
        
    def test_only_corporate(self):
        # Case: Only corporate
        p_counts = {1: 100, 2: 50}
        p_names = {1: "GARDEN HILL APTS, LLC", 2: "ANOTHER REALTY INC"}
        
        name = get_network_name(p_counts, p_names)
        print(f"Test 3 Result: {name}")
        self.assertEqual(name, "GARDEN HILL APTS, LLC")

    def test_corporate_keyword_embedded(self):
        # Case: "REALTY" in name
        p_counts = {1: 100, 2: 50}
        p_names = {1: "SMITH REALTY GROUP", 2: "JOHN SMITH"}
        name = get_network_name(p_counts, p_names)
        print(f"Test 4 Result: {name}")
        self.assertEqual(name, "JOHN SMITH")

if __name__ == '__main__':
    unittest.main()
