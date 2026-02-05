import os
import psycopg2
from psycopg2.extras import execute_values
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")

# --- CLASSIFIED LISTS ---
# (Email lists are unchanged and correct)
PUBLIC_EMAIL_PROVIDERS = [
    'gmail.com', 'yahoo.com', 'aol.com', 'hotmail.com', 'sbcglobal.net', 
    'comcast.net', 'outlook.com', 'optonline.net', 'icloud.com', 'snet.net', 
    'att.net', 'cox.net', 'msn.com', 'me.com', 'live.com', 'charter.net', 
    'mac.com', 'earthlink.net', 'ymail.com', 'verizon.net', 'optimum.net', 
    'protonmail.com', 'mail.com', 'juno.com', 'proton.me', 'rocketmail.com', 
    'cs.com', 'frontier.com', 'myyahoo.com', 'aim.com', 'mindspring.com', 
    'prodigy.net', 'netscape.net', 'gmx.com', 'bellsouth.net', 'usa.net', 
    'netzero.net', 'ct.metrocast.net', 'atlanticbb.net', 'hotmail.es', 
    'netzero.com', 'usa.com', 'optionline.net', 'pm.me', 'yahoo.com.br', 
    'yahoo.co.uk', 'ix.netcom.com', 'email.com', 'inbox.com', 'yahoo.es', 
    'google.com', 'mail.ru', 'nyc.rr.com', 'peoplepc.com', 'fastmail.com', 
    'reagan.com', 'charter.com', 'rcn.com', 'roadrunner.com', 'excite.com', 
    'erols.com', 'pobox.com', 'sbcglobal.com'
]
REGISTRAR_DOMAINS = [
    'wolterskluwer.com', 'incfile.com', 'cscinfo.com', 'cscglobal.com', 
    'northwestregisteredagent.com', 'cogencyglobal.com', 'registeredagentsinc.com', 
    'durangoagency.com', 'corpcreations.com', 'rasi.com', 'primecorporateservices.com', 
    'incorp.com', 'unitedagentgroup.com', 'corporatedocfiling.com', 
    'capitolservices.com', 'vcorpservices.com', 'acs123.com', 'corpnet.com', 
    'mycorporation.com', 'newbusinessfiling.org', 'urscompliance.com', 
    'statefilings.in', 'multiservicesintl.com', 'eminutes.com', 'incserv.com', 
    'spinrep.com', 'zenbusiness.com', 'rohuer.com', 'xxx.com', 'andersonadvisors.com', 
    'premiercfs.com', 'summertax.com', 'atlantistaxllc.com', 'cl-law.com', 
    'mycompanyworks.com', 'betterlegal.com', 'myparacorp.com', 'bizfilings.com', 
    'accelcompliance.com', 'nationalcorp.com', 'nchinc.com', 'usa-llc-filing.com', 
    'agent.middesk.com', 'corporatedirect.com', 'platinumfilings.com', 'filejet.com', 
    'nucofilings.com', 'totallegal.com', 'wyomingllcattorney.com', 'teloslegalcorp.com', 
    'corpco.com', 'singlefile.io', 'kkoslawyers.com', 'fileitusa.com', 
    'annualregistration.com', 'harborcompliance.com', 'cloudpeaklaw.com', 
    'rushfiling.com', 'velawcityinc.com', 'amerilawyer.com', 'taftlaw.com', 
    'delaneycorporate.com', 'licensesure.biz', 'profilefilings.com', 
    'incauthority.com', 'vstatefilings.com', 'interstatefilings.com', 'foundationsource.com', 
    'pattoncompliance.com', 'commpliancegroup.com', 'fileacorp.com', 'rsfilings.com', 
    'clasinfo.com', 'directincorp.com', 'myllc.com', 'teloslegalcorp.com', 
    'cornerstonelicensing.com', 'sessions.legal', 'corpomax.com', 'domyllc.com', 
    'tmf-group.com', 'fileitusa.com', 'mercury-biz.com', 'wyomingllcattorney.com', 
    'amerilawyer.com', 'totalbizsolutions.net', 'connecticutregisteredagent.com', 
    'murthalaw.com', 'ruccilawgroup.com', 'halloransage.com', 'znclaw.com', 'sodlosky.com', 
    'daypitney.com', 'siegeloconnor.com', 'masotti.com', 'weinbergpc.com', 'oberstlaw.com', 
    'tclaw.biz', 'goodwin.com', 'russorizio.com', 'gtlslaw.com', 'sssattorneys.com', 
    'cblawgrp.com', 'csgct.com', 'withersworldwide.com', 'cohenandwolf.com', 'pullcom.com', 
    'gouldratner.com', 'wiggin.com', 'hinckleyallen.com', 'carmodylaw.com', 'bymlaw.com', 
    'garfunkelwild.com', 'ctm-law.com', 'lobo-law.com', 'wallersmithpalmer.com', 'flb.law', 
    'proskauer.com', 'polsinelli.com', 'faegredrinker.com', 'bjslawyers.com', 'csd-law.com', 
    'meltzerlippe.com', 'bennettcocpa.com', 'iselaccounting.com', 'wycpas.com', 'henrymensahcpa.com', 
    'bracctax.com', 'ciampitax.com', 'mjscpa.com', 'jassbofinancial.com', 'l-hcpas.com', 
    'fkcpas.com', 'rghcpa.com', 'ssfpc.com', 'whgcpa.com', 'amtax.cpa', 'marcumllp.com', 
    'federicosettecpa.com', 'baileymoore.com', 'barrongannon.com', 'skpadvisors.com', 'cbiz.com', 
    'claconnect.com', 'deloitte.com', 'ey.com', 'pkfod.com', 'citrincooperman.com', 'pragermetis.com', 
    'uhy-us.com', 'eisneramper.com', 'reynoldsrowella.com', 'kpgcpa.com', 'mahoneysabol.com', 
    'none.com', 'xxx.com.', 'gamil.com', 'qq.com', 'ge.com', 'hhchealth.org', 'allstate.com', 
    'kw.com', 'raveis.com', 'sunnova.com', 'diobpt.org', 'wellsfargo.com', 'ventasreit.com', 
    'cbre.com', 'gs.com', 'remax.net', 'trinityhealthofne.org', 'statefarm.com', 'adp.com', 
    'homeservices.com', 'paychex.com', 'compass.com', 'hrblock.com', 'aig.com', 'cigna.com', 
    'wm.com', 'claconnect.com', 'teamhealth.com', 'farmersagent.com', 'americantower.com', 
    'transamerica.com', 'usbank.com', 'sedgwick.com', 'vizientinc.com', 'bankofamerica.com', 
    'jpmchase.com', 'metlife.com', 'fedex.com', 'aramark.com', 'sodexo.com', 'cbmoves.com', 
    'uhaul.com', 'erac.com', 'thehartford.com', 'jci.com', 'lmco.com', 'us.hsbc.com', 
    'macys.com', 'ford.com', 'mtb.com', 'medtronic.com', 'websterbank.com', 'us.ibm.com', 
    'mastec.com', 'nrg.com', 'asplundh.com', 'theupsstore.com', 'citi.com', 'nm.com', 
    'compass-usa.com', 'unitedcorporate.com', 'maples.com', 'zedra.com', 'uragents.com', 
    'telyon.com', 'nfp.com', 'kpmg.com', 'pwc.com', 'raymondjames.com', 'lpl.com', 

    'registeredagent.com', 'registered-agent.com', 'northwestregisteredagent.com',
    'legalinc.com', 'vstatefilings.com', 'interstatefilings.com'
]

CUSTOM_NETWORK_DOMAINS = [
    'starwood.com', 'bltoffice.com', 'belpointe.com', 'thepropertygroup.net', 
    'carabetta.com', 'simonkonover.com', 'mandymanagement.com', 'navarinoproperty.com',
    'rms-companies.com', 'chaseenterprises.com', 'lexingtonct.com', 'klebanproperties.com',
    'belfonti.com', 'farnamgroup.com', 'scalzoproperty.com', 'genesishcc.com',
    'lazparking.com', 'propark.com', 'bozzutos.com', 'wakefern.com', 'fiebergroup.com',
    'centerplan.com', 'fusco.com', 'trefz-corp.com', 'fdrich.com', 'manafort.com',
    'dattco.com', 'gaultfamilyco.com', 'lovleydevelopment.com', 'seaboardproperties.com',
    'newcastlehotels.com', 'waterfordhotelgroup.com', 'scalzogroup.com',
    'athenahealthcare.com', 'apple-rehab.com', 'towerfunding.net'
]
REGISTRAR_ADDRESS_SUBSTRINGS = [
    'NO INFORMATION PROVIDED', 'NONE', 'N/A', 'NOT APPLICABLE', 'UNKNOWN', 
    'NOT PROVIDED', 'PO BOX', 'C/O ', 'CARE OF ', 'C.O. ', 'C O ', 
    'CORPORATION TRUST', 'CORPORATION SERVICE COMPANY', 'REGISTERED AGENT', 
    'INCFILE.COM', 'NORTHWEST REGISTERED AGENT', 'UNITED CORPORATE SERVICES', 
    'ZENBUSINESS', 'NATIONAL CORPORATE RESEARCH', 'NATIONAL REGISTERED AGENTS, INC', 
    '1209 ORANGE ST', '2711 CENTERVILLE RD', '615 S DUPONT HWY', 
    '615 SOUTH DUPONT HIGHWAY', '1013 CENTRE RD', '1013 CENTRE ROAD', 
    '3225 MCLEOD DR', '317 WEST AVE #113197', '2389 MAIN ST', 
    '591 WEST PUTNAM AVENUE', '777 SUMMER STREET', '430 NEW PARK AVE', 
    'ONE FINANCIAL PLAZA', '225 ASYLUM STREET', 'ONE CONSTITUTION PLAZA', '101 PEARL STREET'
]

# --- NEW: CATEGORY 4: Principals to IGNORE ---
# High-volume agents, lawyers, or businesses-as-principals that falsely link networks.
# We will NOT add these to the graph. This is the fix.
PRINCIPAL_IGNORE_LIST = [
    # Businesses-as-Principals from your log (these are agents)
    # High-volume agent individuals from your log
    'ALICE M GENIN', 'ALICE M. GENIN', 'PETER M. BRESTOVAN', 'ZENON P. LANKOWSKY',
    'MELANIE K. LUKER', 'THOMAS S. MOFFATT', 'KAREN AZUCENA ESTRADA', 'GEOFFREY W. SAGER',
    'KAREN E. GORTON', 'ELLEN M. BUCKLEY', 'WILLIAM B. MARTIN', 'CLAYTON H. FOWLER',
    'JEROME C. SILVEY', 'PETER M. PHILLIPES', 'Registered Agents Inc', 'Jayne Rothman',
    'Duncan W. McQueen', 'WILLIAM WAYNE RAND', 'Heather Anastasia Lang', 'David Searle',
    'WILLIAM J. GRIZE', 'Margaret Fitzgerald', 'Marilyn Victoria Hirsch', 'CAROL A. DENALE',
    'DOUGLAS L. POLING', 'Randy Larsen', 'ELIZABETH MOOTS', 'STEVEN P. BAUM',
    
    # Common Junk values
    'X',
    
    # Law Firms / Attorneys (Generic blocks)
    'KB LAW', 'KB LAW, LLC', 'KB LAW LLC',
    'ATTORNEY AT LAW', 'LAW OFFICE', 'LAW GROUP', 'LEGAL SERVICES'
]


# --- Database Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("✅ Database connection successful.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def setup_rule_tables(conn):
    """Creates/re-creates all three rule tables."""
    logger.info("Creating matching rule tables...")
    try:
        with conn.cursor() as cursor:
            # Drop tables FIRST
            cursor.execute("DROP TABLE IF EXISTS email_match_rules CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS address_match_rules CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS principal_ignore_list CASCADE;") # --- NEW ---
            
            # Drop types
            cursor.execute("DROP TYPE IF EXISTS email_match_type CASCADE;")
            cursor.execute("DROP TYPE IF EXISTS address_match_type CASCADE;")

            # Create new types and tables
            logger.info("Creating new types and tables...")
            cursor.execute("CREATE TYPE email_match_type AS ENUM ('public', 'registrar', 'custom');")
            cursor.execute("""
                CREATE TABLE email_match_rules (
                    id SERIAL PRIMARY KEY, domain TEXT NOT NULL UNIQUE, match_type email_match_type NOT NULL
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_rules_domain ON email_match_rules(domain);")
            
            cursor.execute("CREATE TYPE address_match_type AS ENUM ('registrar');")
            cursor.execute("""
                CREATE TABLE address_match_rules (
                    id SERIAL PRIMARY KEY, substring TEXT NOT NULL UNIQUE, match_type address_match_type NOT NULL
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_address_rules_substring ON address_match_rules(substring);")

            # --- NEW: Create principal ignore table ---
            cursor.execute("""
                CREATE TABLE principal_ignore_list (
                    id SERIAL PRIMARY KEY,
                    normalized_name TEXT NOT NULL UNIQUE
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_principal_ignore_name ON principal_ignore_list(normalized_name);")
        
        conn.commit()
        logger.info("✅ All rule tables and types are ready.")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error creating tables: {e}")
        raise

def populate_rules(conn):
    """Populates all three rule tables."""
    try:
        with conn.cursor() as cursor:
            # --- Populate Email Rules ---
            logger.info(f"Populating 'email_match_rules'...")
            email_data = []
            for domain in PUBLIC_EMAIL_PROVIDERS: email_data.append((domain, 'public'))
            for domain in REGISTRAR_DOMAINS: email_data.append((domain, 'registrar'))
            for domain in CUSTOM_NETWORK_DOMAINS: email_data.append((domain, 'custom'))
            
            execute_values(
                cursor, "INSERT INTO email_match_rules (domain, match_type) VALUES %s ON CONFLICT (domain) DO NOTHING", email_data
            )
            logger.info(f"✅ Inserted {len(email_data)} total email rules.")

            # --- Populate Address Rules ---
            logger.info(f"Populating 'address_match_rules'...")
            address_data = [(substring, 'registrar') for substring in REGISTRAR_ADDRESS_SUBSTRINGS]
            execute_values(
                cursor, "INSERT INTO address_match_rules (substring, match_type) VALUES %s ON CONFLICT (substring) DO NOTHING", address_data
            )
            logger.info(f"✅ Inserted {len(address_data)} address registrar rules.")

            # --- NEW: Populate Principal Ignore List ---
            logger.info(f"Populating 'principal_ignore_list'...")
            # We must normalize them before inserting
            from shared_utils import normalize_person_name
            principal_data = [(normalize_person_name(name),) for name in PRINCIPAL_IGNORE_LIST if normalize_person_name(name)]
            
            execute_values(
                cursor, "INSERT INTO principal_ignore_list (normalized_name) VALUES %s ON CONFLICT (normalized_name) DO NOTHING", principal_data
            )
            logger.info(f"✅ Inserted {len(principal_data)} principal ignore rules.")

        conn.commit()
        logger.info("🎉 All matching rules populated successfully.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error populating rules: {e}")
        raise

def main():
    if not DATABASE_URL:
        logger.error("❌ Error: DATABASE_URL environment variable is not set.")
        sys.exit(1)
        
    # --- NEW: Must import from shared_utils ---
    try:
        from shared_utils import normalize_person_name
    except ImportError:
        logger.error("❌ Could not import from shared_utils.py. Ensure it is in the same directory.")
        sys.exit(1)

    conn = None
    try:
        conn = get_db_connection()
        setup_rule_tables(conn)
        populate_rules(conn)
        
    except Exception as e:
        logger.error(f"❌ A critical error occurred in main process: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("🔌 Database connection closed.")

if __name__ == "__main__":
    main()