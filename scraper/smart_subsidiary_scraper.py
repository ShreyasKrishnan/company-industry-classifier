# =============================================================================
# SMART SUBSIDIARY SCRAPER - ULTIMATE EDITION (v3.6)
# =============================================================================
# Key features:
# 1) Google Sheets batch input (and optional parent reference CSV)
# 2) Exhibit discovery for EX-21 and EX-21.1 (with embedded EX-21 fallback)
# 3) Robust SEC fetching with retry/backoff for 403/429/5xx
# 4) CIK lookup cached (downloads company_tickers.json once per session)
# 5) Output split into TWO ZIPs per run:
#      - <SheetName>_Subsidiaries.zip : only subsidiaries CSVs
#      - <SheetName>_Logs.zip         : run_summary + cik_not_found + exhibit_failures
# 6) Handles embedded Exhibit sections via cropping
# =============================================================================

import subprocess
import sys
import re
import time
import csv
import zipfile
from pathlib import Path
from datetime import datetime

# -----------------------------------------------------------------------------
# STEP 1: INSTALL PACKAGES
# -----------------------------------------------------------------------------
def install_packages():
    """Install required packages if not already installed"""
    packages = [
        "sec-edgar-downloader",
        "gspread",
        "beautifulsoup4",
        "requests",
        "google-auth",
    ]

    # pip name -> import name
    import_map = {
        "sec-edgar-downloader": "sec_edgar_downloader",
        "beautifulsoup4": "bs4",
        "google-auth": "google.auth",
    }

    print("Checking required packages...")
    for package in packages:
        try:
            __import__(import_map.get(package, package.replace("-", "_")))
        except ImportError:
            print(f"  Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", package])
    print("Packages ready.\n")

install_packages()

# Imports after installation
import requests
from bs4 import BeautifulSoup
import gspread
from google.auth import default
from sec_edgar_downloader import Downloader

try:
    from google.colab import auth as colab_auth
    from google.colab import files
    IN_COLAB = True
except Exception:
    colab_auth = None
    IN_COLAB = False


# -----------------------------------------------------------------------------
# STEP 2: GLOBAL JURISDICTIONS DATABASE (EMBEDDED)
# -----------------------------------------------------------------------------
class GlobalJurisdictions:
    """Database of countries, states, and provinces for location detection"""

    JURISDICTIONS = {
        'united states': {
            'country': 'United States',
            'states': [
                'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
                'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
                'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
                'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
                'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
                'new hampshire', 'new jersey', 'new mexico', 'new york',
                'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
                'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
                'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
                'west virginia', 'wisconsin', 'wyoming',
                'district of columbia', 'd.c.', 'dc', 'puerto rico', 'guam',
                'u.s. virgin islands', 'virgin islands', 'american samoa',
                'northern mariana islands', 'us virgin islands'
            ],
            'aliases': ['usa', 'u.s.', 'us', 'united states of america']
        },
        'canada': {
            'country': 'Canada',
            'provinces': [
                'alberta', 'british columbia', 'manitoba', 'new brunswick',
                'newfoundland and labrador', 'newfoundland', 'northwest territories',
                'nova scotia', 'nunavut', 'ontario', 'prince edward island',
                'quebec', 'saskatchewan', 'yukon', 'yukon territory'
            ],
            'aliases': ['ca']
        },
        'mexico': {
            'country': 'Mexico',
            'states': [
                'aguascalientes', 'baja california', 'baja california sur',
                'campeche', 'chiapas', 'chihuahua', 'coahuila', 'colima',
                'durango', 'guanajuato', 'guerrero', 'hidalgo', 'jalisco',
                'mexico city', 'mexico state', 'michoacan', 'morelos', 'nayarit',
                'nuevo leon', 'oaxaca', 'puebla', 'queretaro', 'quintana roo',
                'san luis potosi', 'sinaloa', 'sonora', 'tabasco', 'tamaulipas',
                'tlaxcala', 'veracruz', 'yucatan', 'zacatecas'
            ],
            'aliases': ['mx']
        },
        'united kingdom': {
            'country': 'United Kingdom',
            'regions': [
                'england', 'scotland', 'wales', 'northern ireland',
                'london', 'greater london', 'manchester', 'birmingham',
                'liverpool', 'edinburgh', 'glasgow', 'cardiff', 'belfast'
            ],
            'aliases': ['uk', 'great britain', 'britain', 'gb']
        },
        'germany': {
            'country': 'Germany',
            'states': [
                'baden-wurttemberg', 'bavaria', 'berlin', 'brandenburg', 'bremen',
                'hamburg', 'hesse', 'lower saxony', 'mecklenburg-vorpommern',
                'north rhine-westphalia', 'rhineland-palatinate', 'saarland',
                'saxony', 'saxony-anhalt', 'schleswig-holstein', 'thuringia'
            ],
            'aliases': ['de', 'deutschland']
        },
        'france': {
            'country': 'France',
            'regions': [
                'paris', 'ile-de-france', 'provence', 'normandy', 'brittany',
                'alsace', 'lorraine', 'burgundy', 'lyon', 'marseille',
                'toulouse', 'nice', 'nantes', 'strasbourg', 'bordeaux'
            ],
            'aliases': ['fr']
        },
        'italy': {
            'country': 'Italy',
            'regions': [
                'rome', 'lazio', 'lombardy', 'milan', 'venice', 'veneto',
                'tuscany', 'florence', 'naples', 'campania', 'sicily',
                'sardinia', 'piedmont', 'turin', 'emilia-romagna', 'bologna'
            ],
            'aliases': ['it']
        },
        'spain': {
            'country': 'Spain',
            'regions': [
                'madrid', 'catalonia', 'barcelona', 'andalusia', 'valencia',
                'galicia', 'basque country', 'castile and leon', 'castilla y leon',
                'seville', 'bilbao', 'malaga', 'canary islands', 'balearic islands'
            ],
            'aliases': ['es']
        },
        'netherlands': {
            'country': 'Netherlands',
            'provinces': [
                'amsterdam', 'north holland', 'south holland', 'utrecht',
                'rotterdam', 'the hague', 'groningen', 'friesland', 'drenthe',
                'overijssel', 'gelderland', 'flevoland', 'zeeland',
                'north brabant', 'limburg'
            ],
            'aliases': ['nl', 'holland']
        },
        'belgium': {
            'country': 'Belgium',
            'regions': [
                'brussels', 'flanders', 'wallonia', 'antwerp', 'ghent',
                'bruges', 'liege', 'namur', 'charleroi'
            ],
            'aliases': ['be']
        },
        'switzerland': {
            'country': 'Switzerland',
            'cantons': [
                'zurich', 'geneva', 'basel', 'bern', 'lausanne', 'lucerne',
                'st. gallen', 'lugano', 'vaud', 'valais', 'ticino'
            ],
            'aliases': ['ch']
        },
        'austria': {
            'country': 'Austria',
            'states': [
                'vienna', 'lower austria', 'upper austria', 'styria', 'tyrol',
                'carinthia', 'salzburg', 'vorarlberg', 'burgenland'
            ],
            'aliases': ['at']
        },
        'ireland': {
            'country': 'Ireland',
            'counties': [
                'dublin', 'cork', 'galway', 'limerick', 'waterford',
                'kerry', 'clare', 'tipperary', 'mayo', 'donegal'
            ],
            'aliases': ['ie', 'eire']
        },
        'poland': {
            'country': 'Poland',
            'regions': [
                'warsaw', 'krakow', 'wroclaw', 'poznan', 'gdansk',
                'szczecin', 'lodz', 'katowice', 'lublin', 'bialystok'
            ],
            'aliases': ['pl']
        },
        'czech republic': {
            'country': 'Czech Republic',
            'regions': [
                'prague', 'brno', 'ostrava', 'plzen', 'liberec',
                'olomouc', 'ceske budejovice', 'hradec kralove'
            ],
            'aliases': ['czechia', 'cz']
        },
        'portugal': {
            'country': 'Portugal',
            'regions': [
                'lisbon', 'porto', 'algarve', 'madeira', 'azores',
                'braga', 'coimbra', 'setubal', 'funchal'
            ],
            'aliases': ['pt']
        },
        'greece': {
            'country': 'Greece',
            'regions': [
                'athens', 'thessaloniki', 'patras', 'heraklion', 'larissa',
                'crete', 'rhodes', 'corfu', 'santorini'
            ],
            'aliases': ['gr']
        },
        'sweden': {
            'country': 'Sweden',
            'regions': [
                'stockholm', 'gothenburg', 'malmo', 'uppsala', 'vasteras',
                'orebro', 'linkoping', 'helsingborg', 'jonkoping'
            ],
            'aliases': ['se']
        },
        'norway': {
            'country': 'Norway',
            'regions': [
                'oslo', 'bergen', 'trondheim', 'stavanger', 'drammen',
                'fredrikstad', 'kristiansand', 'tromso'
            ],
            'aliases': ['no']
        },
        'denmark': {
            'country': 'Denmark',
            'regions': [
                'copenhagen', 'aarhus', 'odense', 'aalborg', 'esbjerg',
                'randers', 'kolding', 'horsens', 'vejle'
            ],
            'aliases': ['dk']
        },
        'finland': {
            'country': 'Finland',
            'regions': [
                'helsinki', 'espoo', 'tampere', 'vantaa', 'oulu',
                'turku', 'jyvaskyla', 'lahti', 'kuopio'
            ],
            'aliases': ['fi']
        },
        'luxembourg': {
            'country': 'Luxembourg',
            'regions': ['luxembourg city', 'esch-sur-alzette', 'differdange'],
            'aliases': ['lu']
        },
        'china': {
            'country': 'China',
            'provinces': [
                'beijing', 'shanghai', 'guangdong', 'shenzhen', 'guangzhou',
                'chongqing', 'tianjin', 'jiangsu', 'nanjing', 'suzhou',
                'zhejiang', 'hangzhou', 'ningbo', 'shandong', 'qingdao',
                'henan', 'zhengzhou', 'hubei', 'wuhan', 'sichuan', 'chengdu',
                'fujian', 'xiamen', 'fuzhou', 'liaoning', 'dalian', 'shenyang',
                'shaanxi', 'xian', 'hebei', 'shanxi', 'inner mongolia',
                'heilongjiang', 'harbin', 'jilin', 'changchun', 'anhui', 'hefei',
                'jiangxi', 'nanchang', 'hunan', 'changsha', 'guangxi', 'nanning',
                'guizhou', 'guiyang', 'yunnan', 'kunming', 'tibet', 'lhasa',
                'gansu', 'lanzhou', 'qinghai', 'xining', 'ningxia', 'yinchuan',
                'xinjiang', 'urumqi', 'hainan', 'haikou', 'sanya'
            ],
            'aliases': ['cn', 'prc', "people's republic of china"]
        },
        'hong kong': {
            'country': 'Hong Kong',
            'regions': ['hong kong island', 'kowloon', 'new territories'],
            'aliases': ['hk', 'hong kong sar']
        },
        'japan': {
            'country': 'Japan',
            'prefectures': [
                'tokyo', 'osaka', 'kyoto', 'yokohama', 'nagoya', 'sapporo',
                'kobe', 'fukuoka', 'kawasaki', 'saitama', 'hiroshima',
                'sendai', 'chiba', 'kitakyushu', 'sakai', 'niigata',
                'hamamatsu', 'kumamoto', 'okayama', 'sagamihara', 'shizuoka',
                'hokkaido', 'aichi', 'kanagawa', 'hyogo', 'fukushima',
                'ibaraki', 'tochigi', 'gunma', 'mie', 'shiga', 'nara',
                'wakayama', 'tottori', 'shimane', 'okayama', 'yamaguchi',
                'tokushima', 'kagawa', 'ehime', 'kochi', 'saga', 'nagasaki',
                'oita', 'miyazaki', 'kagoshima', 'okinawa'
            ],
            'aliases': ['jp']
        },
        'south korea': {
            'country': 'South Korea',
            'regions': [
                'seoul', 'busan', 'incheon', 'daegu', 'daejeon', 'gwangju',
                'ulsan', 'suwon', 'changwon', 'seongnam', 'goyang', 'yongin',
                'bucheon', 'ansan', 'cheongju', 'jeonju', 'anyang', 'pohang',
                'gyeonggi', 'gangwon', 'north chungcheong', 'south chungcheong',
                'north jeolla', 'south jeolla', 'north gyeongsang', 'south gyeongsang',
                'jeju'
            ],
            'aliases': ['korea', 'republic of korea', 'kr', 'rok']
        },
        'india': {
            'country': 'India',
            'states': [
                'andhra pradesh', 'arunachal pradesh', 'assam', 'bihar',
                'chhattisgarh', 'goa', 'gujarat', 'haryana', 'himachal pradesh',
                'jharkhand', 'karnataka', 'kerala', 'madhya pradesh',
                'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                'odisha', 'punjab', 'rajasthan', 'sikkim', 'tamil nadu',
                'telangana', 'tripura', 'uttar pradesh', 'uttarakhand',
                'west bengal',
                'delhi', 'new delhi', 'mumbai', 'bangalore', 'bengaluru',
                'hyderabad', 'chennai', 'kolkata', 'pune', 'ahmedabad',
                'jaipur', 'surat', 'lucknow', 'kanpur', 'nagpur', 'indore',
                'thane', 'bhopal', 'visakhapatnam', 'pimpri-chinchwad',
                'patna', 'vadodara', 'ghaziabad', 'ludhiana', 'agra',
                'nashik', 'faridabad', 'meerut', 'rajkot', 'kalyan-dombivali',
                'vasai-virar', 'varanasi', 'srinagar', 'aurangabad', 'dhanbad',
                'amritsar', 'navi mumbai', 'allahabad', 'ranchi', 'howrah',
                'coimbatore', 'jabalpur', 'gwalior', 'vijayawada', 'jodhpur',
                'madurai', 'raipur', 'kota', 'chandigarh', 'guwahati',
                'puducherry', 'jammu and kashmir', 'ladakh', 'andaman and nicobar',
                'lakshadweep', 'dadra and nagar haveli', 'daman and diu'
            ],
            'aliases': ['in']
        },
        'singapore': {
            'country': 'Singapore',
            'regions': ['singapore', 'central region', 'east region', 'north region', 'west region'],
            'aliases': ['sg']
        },
        'malaysia': {
            'country': 'Malaysia',
            'states': [
                'kuala lumpur', 'selangor', 'johor', 'penang', 'perak',
                'kedah', 'kelantan', 'terengganu', 'pahang', 'negeri sembilan',
                'malacca', 'melaka', 'sabah', 'sarawak', 'labuan',
                'putrajaya', 'perlis'
            ],
            'aliases': ['my']
        },
        'thailand': {
            'country': 'Thailand',
            'provinces': [
                'bangkok', 'chiang mai', 'phuket', 'pattaya', 'chonburi',
                'nonthaburi', 'samut prakan', 'udon thani', 'nakhon ratchasima',
                'khon kaen', 'surat thani', 'songkhla', 'hat yai'
            ],
            'aliases': ['th']
        },
        'indonesia': {
            'country': 'Indonesia',
            'provinces': [
                'jakarta', 'java', 'bali', 'surabaya', 'bandung', 'medan',
                'semarang', 'makassar', 'palembang', 'tangerang', 'depok',
                'bekasi', 'west java', 'east java', 'central java',
                'yogyakarta', 'banten', 'sumatra', 'kalimantan', 'sulawesi',
                'papua', 'maluku', 'nusa tenggara'
            ],
            'aliases': ['id']
        },
        'philippines': {
            'country': 'Philippines',
            'regions': [
                'manila', 'metro manila', 'quezon city', 'makati', 'cebu',
                'davao', 'caloocan', 'zamboanga', 'taguig', 'pasig',
                'cagayan de oro', 'paranaque', 'las pinas', 'antipolo',
                'luzon', 'visayas', 'mindanao', 'national capital region'
            ],
            'aliases': ['ph', 'phillipines']
        },
        'vietnam': {
            'country': 'Vietnam',
            'regions': [
                'hanoi', 'ho chi minh city', 'saigon', 'da nang', 'hai phong',
                'can tho', 'bien hoa', 'hue', 'nha trang', 'buon ma thuot',
                'vung tau', 'nam dinh', 'qui nhon'
            ],
            'aliases': ['vn']
        },
        'taiwan': {
            'country': 'Taiwan',
            'regions': [
                'taipei', 'new taipei', 'taichung', 'tainan', 'kaohsiung',
                'taoyuan', 'hsinchu', 'keelung', 'chiayi', 'changhua'
            ],
            'aliases': ['tw', 'republic of china', 'roc']
        },
        'australia': {
            'country': 'Australia',
            'states': [
                'new south wales', 'nsw', 'victoria', 'vic', 'queensland', 'qld',
                'south australia', 'sa', 'western australia', 'wa',
                'tasmania', 'tas', 'northern territory', 'nt',
                'australian capital territory', 'act',
                'sydney', 'melbourne', 'brisbane', 'perth', 'adelaide',
                'gold coast', 'newcastle', 'canberra', 'sunshine coast',
                'wollongong', 'hobart', 'geelong', 'townsville', 'cairns',
                'darwin', 'toowoomba', 'ballarat', 'bendigo', 'albury'
            ],
            'aliases': ['au']
        },
        'new zealand': {
            'country': 'New Zealand',
            'regions': [
                'auckland', 'wellington', 'christchurch', 'hamilton', 'tauranga',
                'dunedin', 'palmerston north', 'napier', 'hastings', 'nelson',
                'rotorua', 'new plymouth', 'whangarei', 'invercargill',
                'north island', 'south island'
            ],
            'aliases': ['nz']
        },
        'united arab emirates': {
            'country': 'United Arab Emirates',
            'emirates': [
                'dubai', 'abu dhabi', 'sharjah', 'ajman', 'umm al-quwain',
                'ras al-khaimah', 'fujairah'
            ],
            'aliases': ['uae', 'emirates']
        },
        'saudi arabia': {
            'country': 'Saudi Arabia',
            'regions': [
                'riyadh', 'jeddah', 'mecca', 'medina', 'dammam', 'khobar',
                'tabuk', 'buraidah', 'khamis mushait', 'hail', 'najran',
                'jubail', 'abha', 'yanbu', 'al-kharj'
            ],
            'aliases': ['sa', 'ksa']
        },
        'israel': {
            'country': 'Israel',
            'regions': [
                'tel aviv', 'jerusalem', 'haifa', 'rishon lezion', 'petah tikva',
                'ashdod', 'netanya', 'beersheba', 'holon', 'bnei brak',
                'ramat gan', 'ashkelon', 'rehovot', 'bat yam', 'herzliya'
            ],
            'aliases': ['il']
        },
        'qatar': {
            'country': 'Qatar',
            'regions': ['doha', 'al rayyan', 'al wakrah', 'al khor', 'mesaieed'],
            'aliases': ['qa']
        },
        'kuwait': {
            'country': 'Kuwait',
            'regions': ['kuwait city', 'hawalli', 'salmiya', 'sabah al-salem', 'farwaniya'],
            'aliases': ['kw']
        },
        'bahrain': {
            'country': 'Bahrain',
            'regions': ['manama', 'muharraq', 'riffa', 'hamad town', 'isa town'],
            'aliases': ['bh']
        },
        'oman': {
            'country': 'Oman',
            'regions': ['muscat', 'salalah', 'sohar', 'nizwa', 'sur', 'ibri'],
            'aliases': ['om']
        },
        'turkey': {
            'country': 'Turkey',
            'provinces': [
                'istanbul', 'ankara', 'izmir', 'bursa', 'adana', 'gaziantep',
                'konya', 'antalya', 'kayseri', 'mersin', 'eskisehir', 'diyarbakir',
                'samsun', 'denizli', 'sanliurfa', 'adapazari', 'malatya', 'kahramanmaras'
            ],
            'aliases': ['tr', 'turkiye']
        },
        'brazil': {
            'country': 'Brazil',
            'states': [
                'sao paulo', 'rio de janeiro', 'brasilia', 'salvador', 'fortaleza',
                'belo horizonte', 'manaus', 'curitiba', 'recife', 'porto alegre',
                'belem', 'goiania', 'guarulhos', 'campinas', 'sao luis',
                'sao goncalo', 'maceio', 'duque de caxias', 'natal', 'teresina',
                'acre', 'alagoas', 'amapa', 'amazonas', 'bahia', 'ceara',
                'distrito federal', 'espirito santo', 'goias', 'maranhao',
                'mato grosso', 'mato grosso do sul', 'minas gerais', 'para',
                'paraiba', 'parana', 'pernambuco', 'piaui', 'rio grande do norte',
                'rio grande do sul', 'rondonia', 'roraima', 'santa catarina',
                'sergipe', 'tocantins'
            ],
            'aliases': ['br']
        },
        'argentina': {
            'country': 'Argentina',
            'provinces': [
                'buenos aires', 'cordoba', 'rosario', 'mendoza', 'la plata',
                'san miguel de tucuman', 'mar del plata', 'salta', 'santa fe',
                'san juan', 'resistencia', 'santiago del estero', 'corrientes',
                'posadas', 'bahia blanca', 'parana', 'neuquen', 'formosa',
                'catamarca', 'chaco', 'chubut', 'entre rios', 'jujuy',
                'la pampa', 'la rioja', 'misiones', 'rio negro', 'san luis',
                'santa cruz', 'tierra del fuego'
            ],
            'aliases': ['ar']
        },
        'chile': {
            'country': 'Chile',
            'regions': [
                'santiago', 'valparaiso', 'concepcion', 'la serena', 'antofagasta',
                'temuco', 'rancagua', 'talca', 'arica', 'puerto montt',
                'chillan', 'iquique', 'los angeles', 'punta arenas', 'copiapo',
                'valdivia', 'osorno', 'quillota', 'calama', 'curico'
            ],
            'aliases': ['cl']
        },
        'colombia': {
            'country': 'Colombia',
            'departments': [
                'bogota', 'medellin', 'cali', 'barranquilla', 'cartagena',
                'cucuta', 'bucaramanga', 'pereira', 'santa marta', 'ibague',
                'pasto', 'manizales', 'neiva', 'villavicencio', 'armenia',
                'antioquia', 'atlantico', 'bolivar', 'boyaca', 'caldas',
                'caqueta', 'cauca', 'cesar', 'choco', 'cordoba', 'cundinamarca',
                'huila', 'la guajira', 'magdalena', 'meta', 'narino',
                'norte de santander', 'quindio', 'risaralda', 'santander',
                'sucre', 'tolima', 'valle del cauca'
            ],
            'aliases': ['co']
        },
        'peru': {
            'country': 'Peru',
            'regions': [
                'lima', 'arequipa', 'trujillo', 'chiclayo', 'piura', 'iquitos',
                'cusco', 'huancayo', 'chimbote', 'pucallpa', 'tacna', 'ica',
                'juliaca', 'sullana', 'ayacucho', 'cajamarca', 'puno', 'huanuco'
            ],
            'aliases': ['pe']
        },
        'venezuela': {
            'country': 'Venezuela',
            'states': [
                'caracas', 'maracaibo', 'valencia', 'barquisimeto', 'maracay',
                'ciudad guayana', 'barcelona', 'maturin', 'puerto la cruz',
                'petare', 'turmero', 'merida', 'san cristobal', 'ciudad bolivar'
            ],
            'aliases': ['ve']
        },
        'ecuador': {
            'country': 'Ecuador',
            'provinces': [
                'quito', 'guayaquil', 'cuenca', 'santo domingo', 'machala',
                'manta', 'portoviejo', 'loja', 'ambato', 'esmeraldas',
                'quevedo', 'riobamba', 'milagro', 'ibarra'
            ],
            'aliases': ['ec']
        },
        'uruguay': {
            'country': 'Uruguay',
            'departments': [
                'montevideo', 'salto', 'paysandu', 'las piedras', 'rivera',
                'maldonado', 'tacuarembo', 'melo', 'artigas', 'mercedes',
                'minas', 'san jose', 'durazno', 'florida', 'treinta y tres'
            ],
            'aliases': ['uy']
        },
        'paraguay': {
            'country': 'Paraguay',
            'departments': [
                'asuncion', 'ciudad del este', 'san lorenzo', 'luque',
                'capiata', 'lambare', 'fernando de la mora', 'limpio',
                'nemby', 'encarnacion', 'pedro juan caballero'
            ],
            'aliases': ['py']
        },
        'bolivia': {
            'country': 'Bolivia',
            'departments': [
                'la paz', 'santa cruz', 'cochabamba', 'sucre', 'oruro',
                'potosi', 'tarija', 'trinidad', 'cobija'
            ],
            'aliases': ['bo']
        },
        'costa rica': {
            'country': 'Costa Rica',
            'provinces': [
                'san jose', 'alajuela', 'cartago', 'heredia', 'limon',
                'puntarenas', 'guanacaste'
            ],
            'aliases': ['cr']
        },
        'panama': {
            'country': 'Panama',
            'provinces': [
                'panama city', 'colon', 'david', 'la chorrera', 'santiago',
                'bocas del toro', 'chiriqui', 'cocle', 'darien', 'herrera',
                'los santos', 'veraguas'
            ],
            'aliases': ['pa']
        },
        'cayman islands': {
            'country': 'Cayman Islands',
            'regions': ['grand cayman', 'cayman brac', 'little cayman', 'george town'],
            'aliases': ['ky']
        },
        'bermuda': {
            'country': 'Bermuda',
            'regions': ['hamilton', 'st. george'],
            'aliases': ['bm']
        },
        'bahamas': {
            'country': 'Bahamas',
            'regions': ['nassau', 'freeport', 'grand bahama', 'abaco', 'eleuthera'],
            'aliases': ['bs', 'the bahamas']
        },
        'barbados': {
            'country': 'Barbados',
            'regions': ['bridgetown', 'speightstown', 'oistins', 'holetown'],
            'aliases': ['bb']
        },
        'jamaica': {
            'country': 'Jamaica',
            'parishes': [
                'kingston', 'spanish town', 'portmore', 'montego bay',
                'mandeville', 'may pen', 'st. andrew', 'st. catherine',
                'st. james', 'clarendon', 'manchester', 'westmoreland'
            ],
            'aliases': ['jm']
        },
        'trinidad and tobago': {
            'country': 'Trinidad and Tobago',
            'regions': [
                'port of spain', 'san fernando', 'chaguanas', 'arima',
                'point fortin', 'scarborough', 'tobago'
            ],
            'aliases': ['tt', 'trinidad', 'tobago']
        },
        'british virgin islands': {
            'country': 'British Virgin Islands',
            'regions': ['road town', 'tortola', 'virgin gorda', 'anegada', 'jost van dyke'],
            'aliases': ['bvi', 'vg']
        },
        'turks and caicos': {
            'country': 'Turks and Caicos',
            'regions': ['grand turk', 'providenciales', 'cockburn town'],
            'aliases': ['tc', 'turks and caicos islands']
        },
        'south africa': {
            'country': 'South Africa',
            'provinces': [
                'johannesburg', 'cape town', 'durban', 'pretoria', 'port elizabeth',
                'bloemfontein', 'east london', 'pietermaritzburg', 'nelspruit',
                'kimberley', 'polokwane', 'gauteng', 'western cape', 'eastern cape',
                'kwazulu-natal', 'free state', 'limpopo', 'mpumalanga',
                'north west', 'northern cape'
            ],
            'aliases': ['za']
        },
        'nigeria': {
            'country': 'Nigeria',
            'states': [
                'lagos', 'abuja', 'kano', 'ibadan', 'port harcourt', 'benin city',
                'kaduna', 'jos', 'ilorin', 'aba', 'onitsha', 'warri', 'calabar',
                'abeokuta', 'akure', 'enugu', 'owerri', 'bauchi', 'maiduguri'
            ],
            'aliases': ['ng']
        },
        'kenya': {
            'country': 'Kenya',
            'counties': [
                'nairobi', 'mombasa', 'kisumu', 'nakuru', 'eldoret', 'ruiru',
                'kikuyu', 'thika', 'malindi', 'kitale', 'garissa', 'kakamega'
            ],
            'aliases': ['ke']
        },
        'egypt': {
            'country': 'Egypt',
            'governorates': [
                'cairo', 'alexandria', 'giza', 'shubra el-kheima', 'port said',
                'suez', 'luxor', 'aswan', 'asyut', 'ismailia', 'faiyum',
                'zagazig', 'damietta', 'assiut', 'minya', 'damanhur'
            ],
            'aliases': ['eg']
        },
        'russia': {
            'country': 'Russia',
            'regions': [
                'moscow', 'st. petersburg', 'novosibirsk', 'yekaterinburg',
                'nizhny novgorod', 'kazan', 'chelyabinsk', 'omsk', 'samara',
                'rostov-on-don', 'ufa', 'krasnoyarsk', 'voronezh', 'perm',
                'volgograd', 'krasnodar', 'saratov', 'tyumen', 'tolyatti'
            ],
            'aliases': ['ru', 'russian federation']
        },
        'ukraine': {
            'country': 'Ukraine',
            'regions': [
                'kyiv', 'kiev', 'kharkiv', 'odessa', 'dnipro', 'donetsk',
                'zaporizhzhia', 'lviv', 'kryvyi rih', 'mykolaiv', 'mariupol',
                'luhansk', 'vinnytsia', 'simferopol', 'kherson', 'poltava'
            ],
            'aliases': ['ua']
        },
        'kazakhstan': {
            'country': 'Kazakhstan',
            'regions': [
                'almaty', 'nur-sultan', 'astana', 'shymkent', 'karaganda',
                'aktobe', 'taraz', 'pavlodar', 'ust-kamenogorsk', 'semey',
                'atyrau', 'kostanay', 'kyzylorda', 'oral', 'petropavl'
            ],
            'aliases': ['kz']
        }
    }

    @classmethod
    def find_country(cls, text):
        if not text: return "Not specified", "Not specified"
        clean_text = text.lower().strip().replace('.', '')
        for country, data in cls.JURISDICTIONS.items():
            if clean_text == country or clean_text in data.get('aliases', []):
                return data['country'], text.strip()

            # Check regions inside country
            for key in data:
                if isinstance(data[key], list) and clean_text in data[key]:
                    return data['country'], text.strip()

        return text.strip(), text.strip()

# -----------------------------------------------------------------------------
# STEP 3: MAIN SCRAPER CLASS
# -----------------------------------------------------------------------------
class SmartSubsidiaryScraper:
    def __init__(self, email=None, debug=False):
        if not email:
            print("SEC requires a User-Agent email address.")
            email = input("Enter your email: ").strip()

        self.email = email
        self.debug = debug

        self.downloader = Downloader("ZoomInfo-Research", email)

        # SEC headers: keep minimal and compliant
        self.headers = {
            "User-Agent": self.email,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        }

        # Google Sheets
        self.gc = None
        self.spreadsheet = None

        # Optional parent reference
        self.parent_company_data = {}

        # CIK lookup caching
        self._ticker_cik_map = None
        self._ticker_cik_map_loaded_at = None

        # Per-run tracking
        self.run_id = None
        self.run_started_at = None
        self.run_results = []

        self._init_data_sources()

    # -------------------------------------------------------------------------
    # DATA INIT / REFERENCE LOAD
    # -------------------------------------------------------------------------
    def _init_data_sources(self):
        print("\n" + "=" * 50)
        print("DATA INITIALIZATION")
        print("=" * 50)

        if IN_COLAB:
            print("\n[Optional] Upload Parent Company Reference CSV?")
            try:
                uploaded = files.upload()
                if uploaded:
                    filename = list(uploaded.keys())[0]
                    self.load_parent_company_reference(filename)
            except Exception:
                pass
        else:
            csvs = list(Path(".").glob("*Fortune*.csv")) + list(Path(".").glob("*companies*.csv"))
            if csvs:
                self.load_parent_company_reference(str(csvs[0]))

    def load_parent_company_reference(self, csv_path):
        print(f"  Loading reference data from {csv_path}...")
        try:
            with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    row = {k.upper().strip(): v for k, v in row.items() if k}

                    ticker = row.get("TICKER", "").strip().upper()
                    ticker = re.sub(r"^(NYSE|NASDAQ|AMEX)[:\s]+", "", ticker).strip()

                    if ticker:
                        company_id = row.get("COMPANY_ID") or row.get("CO_ID") or ""
                        url = row.get("URL") or row.get("WEBSITE") or ""

                        self.parent_company_data[ticker] = {
                            "company_id": company_id,
                            "name": row.get("FULL_NAME", row.get("DISPLAY_NAME", "")),
                            "url": url,
                            "rank": row.get("RANK", ""),
                        }
                        count += 1
                print(f"  Loaded {count} parent companies.")
        except Exception as e:
            print(f"  Error loading CSV: {e}")

    # -------------------------------------------------------------------------
    # RUN CONTROL
    # -------------------------------------------------------------------------
    def _new_run(self):
        self.run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.run_started_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.run_results = []

    # -------------------------------------------------------------------------
    # UTILITIES
    # -------------------------------------------------------------------------
    def clean_text(self, text):
        if not text:
            return ""
        text = text.replace("\xa0", " ").replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", text).strip()

    def is_numeric_or_symbol(self, text):
        clean = text.replace("%", "").replace(".", "").replace(",", "").strip()
        if clean.isdigit():
            return True
        if len(text) < 2 and not text.isalpha():
            return True
        return False

    def safe_name(self, s, max_len=80):
        s = re.sub(r"[^\w\s-]", "", str(s or "")).strip()
        s = re.sub(r"\s+", "_", s)
        return s[:max_len] if len(s) > max_len else s

    # -------------------------------------------------------------------------
    # SEC HTTP HELPER (RETRIES / BACKOFF)
    # -------------------------------------------------------------------------
    def fetch_sec(self, url, method="GET", max_retries=6, timeout=45):
        """
        SEC-safe fetch with backoff.
        - Retries on 403/429 and 5xx.
        - Uses Retry-After header if present.
        """
        for attempt in range(1, max_retries + 1):
            try:
                if method.upper() == "HEAD":
                    r = requests.head(url, headers=self.headers, timeout=timeout, allow_redirects=True)
                else:
                    r = requests.get(url, headers=self.headers, timeout=timeout)

                if r.status_code in (403, 429):
                    wait = 2 ** attempt
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        wait = max(wait, int(ra))
                    if self.debug:
                        print(f"  SEC throttled ({r.status_code}). Wait {wait}s -> {url}")
                    time.sleep(wait)
                    continue

                if 500 <= r.status_code < 600:
                    wait = 2 ** attempt
                    if self.debug:
                        print(f"  SEC server error ({r.status_code}). Wait {wait}s -> {url}")
                    time.sleep(wait)
                    continue

                return r

            except Exception as e:
                wait = 2 ** attempt
                if self.debug:
                    print(f"  Fetch error ({e}). Wait {wait}s -> {url}")
                time.sleep(wait)

        raise RuntimeError(f"Failed to fetch after {max_retries} attempts: {url}")

    # -------------------------------------------------------------------------
    # GOOGLE SHEETS
    # -------------------------------------------------------------------------
    def connect_to_spreadsheet(self, sheet_url: str) -> bool:
        print("\nConnecting to Google Spreadsheet...")
        try:
            try:
                if colab_auth:
                    colab_auth.authenticate_user()
                creds, _ = default()
            except Exception:
                creds, _ = default()

            self.gc = gspread.authorize(creds)

            if "/spreadsheets/d/" in sheet_url:
                sheet_id = sheet_url.split("/spreadsheets/d/")[1].split("/")[0]
                self.spreadsheet = self.gc.open_by_key(sheet_id)
            else:
                self.spreadsheet = self.gc.open_by_url(sheet_url)

            print(f"Connected to: {self.spreadsheet.title}")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def read_company_list(self, sheet_name="Input"):
        print(f"\nReading company list from '{sheet_name}'...")
        try:
            try:
                input_sheet = self.spreadsheet.worksheet(sheet_name)
            except Exception:
                input_sheet = self.spreadsheet.sheet1

            values = input_sheet.get_all_values()
            if not values or len(values) < 2:
                return []

            headers = [h.lower() for h in values[0]]
            ticker_col = -1
            name_col = -1

            for i, h in enumerate(headers):
                if "ticker" in h or "symbol" in h:
                    ticker_col = i
                if "company" in h or "name" in h:
                    name_col = i

            if ticker_col == -1:
                ticker_col = 1
            if name_col == -1:
                name_col = 0

            companies = []
            for idx, row in enumerate(values[1:], start=2):
                if len(row) <= ticker_col:
                    continue

                ticker = row[ticker_col].strip().upper()
                sheet_name_val = row[name_col].strip() if len(row) > name_col else ticker

                if not ticker:
                    continue

                ref = self.parent_company_data.get(ticker, {})
                companies.append(
                    {
                        "ticker": ticker,
                        "name": ref.get("name") or sheet_name_val,
                        "company_id": ref.get("company_id", ""),
                        "url": ref.get("url", ""),
                        "row": idx,
                    }
                )

            print(f"Found {len(companies)} companies.")
            return companies
        except Exception as e:
            print(f"Error reading sheet: {e}")
            return []

    # -------------------------------------------------------------------------
    # SEC: CIK LOOKUP (CACHED)
    # -------------------------------------------------------------------------
    def _load_ticker_cik_map(self):
        if self._ticker_cik_map is not None:
            return

        print("Loading SEC ticker->CIK map (cached)...")
        url = "https://www.sec.gov/files/company_tickers.json"
        r = self.fetch_sec(url, method="GET", timeout=45)
        r.raise_for_status()

        data = r.json()
        m = {}
        for entry in data.values():
            t = str(entry.get("ticker", "")).upper().strip()
            cik = str(entry.get("cik_str", "")).zfill(10)
            if t and cik:
                m[t] = cik

        self._ticker_cik_map = m
        self._ticker_cik_map_loaded_at = datetime.utcnow().isoformat() + "Z"

    def get_cik(self, ticker):
        if self.debug:
            print(f"  Looking up CIK for {ticker}...")
        try:
            self._load_ticker_cik_map()
            return self._ticker_cik_map.get(ticker.upper().strip())
        except Exception as e:
            if self.debug:
                print(f"  CIK lookup error: {e}")
            return None

    def get_latest_filing(self, ticker, cik):
        if self.debug:
            print(f"  Checking filings for {ticker}...")
        try:
            self.downloader.get("10-K", ticker, limit=2)
        except Exception as e:
            if self.debug:
                print(f"  Download error: {e}")
            return None

        base_path = Path("sec-edgar-filings") / ticker / "10-K"
        if not base_path.exists():
            return None

        folders = sorted([f for f in base_path.iterdir() if f.is_dir()], reverse=True)
        for folder in folders:
            txt_files = list(folder.glob("*.txt"))
            if not txt_files:
                continue

            with open(txt_files[0], "r", errors="ignore") as f:
                content = f.read(5000)
                acc_match = re.search(r"ACCESSION NUMBER:\s*([0-9\-]+)", content, re.I)
                accession = acc_match.group(1) if acc_match else folder.name
                date_match = re.search(r"FILED AS OF DATE:\s*(\d{8})", content)
                date = date_match.group(1) if date_match else "Unknown"

            return {"cik": cik, "accession": accession, "date": date, "type": "10-K"}

        return None

    # -------------------------------------------------------------------------
    # EXHIBIT URL DISCOVERY: EX-21 and EX-21.1
    # -------------------------------------------------------------------------
    def get_exhibit_urls(self, meta):
        """
        Returns:
          {
            "21":   {"url": "...", "embedded": bool},
            "21.1": {"url": "...", "embedded": bool},
          }
        """
        cik = meta["cik"]
        acc_nodash = meta["accession"].replace("-", "")
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}"
        index_url = f"{base_url}/{meta['accession']}-index.html"

        results = {}

        # 1) Parse filing index
        try:
            r = self.fetch_sec(index_url, method="GET", timeout=45)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, "html.parser")
                for row in soup.find_all("tr"):
                    row_text = row.get_text(" ", strip=True).upper()

                    is_211 = ("EX-21.1" in row_text) or ("EX 21.1" in row_text) or ("EXHIBIT 21.1" in row_text)
                    is_21 = (
                        ("EX-21" in row_text) or ("EX 21" in row_text) or ("EXHIBIT 21" in row_text)
                    ) and (not is_211)

                    if not (is_21 or is_211):
                        continue

                    link = row.find("a", href=True)
                    if not link:
                        continue

                    href = link["href"]
                    url = f"https://www.sec.gov{href}" if href.startswith("/") else f"{base_url}/{href}"

                    if is_211 and "21.1" not in results:
                        results["21.1"] = {"url": url, "embedded": False}
                    elif is_21 and "21" not in results:
                        results["21"] = {"url": url, "embedded": False}
        except Exception:
            pass

        # 2) Filename guesses (best-effort)
        guesses_21 = [
            "ex21.htm", "ex-21.htm", "exhibit21.htm", "subsidiaries.htm",
            "ex21.html", "ex-21.html", "exhibit21.html", "subsidiaries.html",
        ]
        guesses_211 = [
            "ex211.htm", "ex-21.1.htm", "ex21-1.htm", "ex21_1.htm", "ex21.1.htm", "exhibit21_1.htm",
            "ex211.html", "ex-21.1.html", "ex21.1.html", "exhibit21_1.html",
        ]

        if "21" not in results:
            for g in guesses_21:
                url = f"{base_url}/{g}"
                try:
                    hr = self.fetch_sec(url, method="HEAD", timeout=20)
                    if hr.status_code == 200:
                        results["21"] = {"url": url, "embedded": False}
                        break
                except Exception:
                    pass

        if "21.1" not in results:
            for g in guesses_211:
                url = f"{base_url}/{g}"
                try:
                    hr = self.fetch_sec(url, method="HEAD", timeout=20)
                    if hr.status_code == 200:
                        results["21.1"] = {"url": url, "embedded": False}
                        break
                except Exception:
                    pass

        # 3) Embedded fallback for EX-21 if not found as separate
        if "21" not in results:
            try:
                r = self.fetch_sec(index_url, method="GET", timeout=45)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.content, "html.parser")
                    for row in soup.find_all("tr"):
                        if "10-K" in row.get_text(" ", strip=True).upper():
                            link = row.find("a", href=True)
                            if link:
                                href = link["href"]
                                url = f"https://www.sec.gov{href}" if href.startswith("/") else f"{base_url}/{href}"
                                results["21"] = {"url": url, "embedded": True}
                                break
            except Exception:
                pass

        return results

    # -------------------------------------------------------------------------
    # EXTRACTION
    # -------------------------------------------------------------------------
    def extract_subsidiaries(self, html_content, is_embedded=False, exhibit_label="21"):
        soup = BeautifulSoup(html_content, "html.parser")

        if is_embedded:
            if self.debug:
                print(f"  Scanning 10-K for embedded Exhibit {exhibit_label} section...")
            cropped_soup = self._crop_to_exhibit_section(soup, exhibit_label=exhibit_label)
            if cropped_soup:
                soup = cropped_soup
            else:
                if self.debug:
                    print(f"  Could not locate Exhibit {exhibit_label} section in 10-K.")
                return []

        subsidiaries = []

        # Tables
        for table in soup.find_all("table"):
            extracted = self._parse_table(table)
            if extracted:
                subsidiaries.extend(extracted)

        # List pattern fallback
        if not subsidiaries:
            text = soup.get_text("\n")
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            for line in lines:
                match = re.match(r"^([A-Za-z0-9\s\.,&]+)\s+\(([A-Za-z\s]+)\)$", line)
                if match:
                    name = match.group(1).strip()
                    jur = match.group(2).strip()
                    if len(name) > 3 and len(jur) > 2:
                        subsidiaries.append({"name": name, "jurisdiction": jur, "dba": None, "source": "list_pattern"})

        return self._deduplicate(subsidiaries)

    def _crop_to_exhibit_section(self, soup, exhibit_label="21"):
        if exhibit_label == "21.1":
            keywords = [
                re.compile(r"^EXHIBIT\s+21\.1", re.I),
                re.compile(r"^EX-21\.1", re.I),
                re.compile(r"^SUBSIDIARIES OF THE REGISTRANT", re.I),
                re.compile(r"^LIST OF SUBSIDIARIES", re.I),
            ]
        else:
            keywords = [
                re.compile(r"^EXHIBIT\s+21", re.I),
                re.compile(r"^EX-21", re.I),
                re.compile(r"^SUBSIDIARIES OF THE REGISTRANT", re.I),
                re.compile(r"^LIST OF SUBSIDIARIES", re.I),
            ]

        start_node = None
        for tag in soup.find_all(["b", "h1", "h2", "h3", "h4", "p", "div"]):
            text = tag.get_text(strip=True).upper()
            if any(k.search(text) for k in keywords) and len(text) < 140:
                start_node = tag
                break

        if not start_node:
            return None

        content_html = ""
        curr = start_node.next_element
        count = 0
        while curr and count < 7000:
            if curr.name in ["h1", "h2", "h3"] and "EXHIBIT" in curr.get_text(strip=True).upper():
                break
            if hasattr(curr, "name") and curr.name:
                content_html += str(curr)
            curr = curr.next_element
            count += 1

        return BeautifulSoup(content_html, "html.parser")

    def _parse_table(self, table):
        rows = table.find_all("tr")
        if len(rows) < 2:
            return []

        # Detect headers
        headers = []
        header_row_idx = 0
        for i, row in enumerate(rows[:6]):
            cells = row.find_all(["th", "td"])
            texts = [self.clean_text(c.get_text()).lower() for c in cells]
            joined = " ".join(texts)
            if any(k in joined for k in ["subsidiary", "name", "jurisdiction", "state", "doing business", "dba"]):
                headers = texts
                header_row_idx = i
                break

        col_map = {"name": 0, "jur": -1, "dba": None}
        if headers:
            col_map = {"name": None, "jur": None, "dba": None}
            for idx, h in enumerate(headers):
                if "doing business" in h or "dba" in h:
                    col_map["dba"] = idx
                elif "jurisdiction" in h or "state of" in h or "country" in h:
                    col_map["jur"] = idx
                elif "subsidiary" in h or "name" in h or "entity" in h:
                    col_map["name"] = idx

            if col_map["name"] is None:
                used = [v for v in col_map.values() if v is not None]
                for i in range(len(headers)):
                    if i not in used:
                        col_map["name"] = i
                        break

        results = []
        for row in rows[header_row_idx + 1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            cell_texts = [self.clean_text(c.get_text()) for c in cells]
            if col_map["name"] is not None and col_map["name"] >= len(cell_texts):
                continue

            name = cell_texts[col_map["name"]] if col_map["name"] is not None else cell_texts[0]
            jur = (
                cell_texts[col_map["jur"]]
                if col_map["jur"] is not None and col_map["jur"] < len(cell_texts)
                else (cell_texts[-1] if cell_texts else "")
            )
            dba = (
                cell_texts[col_map["dba"]]
                if col_map["dba"] is not None and col_map["dba"] < len(cell_texts)
                else None
            )

            # Fix: numeric/symbol name cell
            if self.is_numeric_or_symbol(name):
                base_idx = (col_map["name"] or 0)
                if len(cell_texts) > base_idx + 1:
                    name = cell_texts[base_idx + 1]

            if not name or len(name) < 2:
                continue

            # Skip header-like row
            if "subsidiary" in name.lower() and "jurisdiction" in jur.lower():
                continue

            # Fix: narrative "name, jurisdiction" in same cell
            if "," in name and (not jur or len(jur) < 3):
                parts = [p.strip() for p in name.split(",") if p.strip()]
                if len(parts) >= 2:
                    name = parts[0]
                    possible_jur = parts[1]
                    if any(x in possible_jur.lower() for x in ["delaware", "california", "texas", "germany", "canada"]):
                        jur = possible_jur

            results.append({"name": name, "jurisdiction": jur, "dba": dba, "source": "table"})

        return results

    def _deduplicate(self, subs):
        seen = set()
        unique = []
        for sub in subs:
            key = (sub.get("name", "") or "").lower().replace(".", "").replace(",", "").strip()
            if key and key not in seen:
                seen.add(key)
                country, clean_jur = GlobalJurisdictions.find_country(sub.get("jurisdiction"))
                sub["country"] = country
                sub["jurisdiction"] = clean_jur
                unique.append(sub)
        return unique

    # -------------------------------------------------------------------------
    # EXPORT: subsidiaries CSV
    # -------------------------------------------------------------------------
    def save_to_csv(self, subsidiaries, company_info, output_dir="subsidiary_csvs"):
        if not subsidiaries:
            return None

        Path(output_dir).mkdir(exist_ok=True)

        ticker = company_info["ticker"]
        name = company_info["name"]
        company_id = company_info.get("company_id", "")

        safe_company = self.safe_name(name, max_len=30)
        filename = f"{ticker}_{safe_company}_{datetime.now().strftime('%Y%m%d')}.csv"
        filepath = Path(output_dir) / filename

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow([
                "Parent_Company_ID", "Parent_Company_Name", "Ticker",
                "Subsidiary_Name", "DBA_Trade_Name", "Jurisdiction", "Country", "Source"
            ])
            for sub in subsidiaries:
                w.writerow([
                    company_id,
                    name,
                    ticker,
                    sub.get("name", ""),
                    sub.get("dba") or "N/A",
                    sub.get("jurisdiction", ""),
                    sub.get("country", ""),
                    sub.get("source", ""),
                ])

        return str(filepath)

    # -------------------------------------------------------------------------
    # EXPORT: log CSVs
    # -------------------------------------------------------------------------
    def write_run_summary_csv(self, output_dir="run_logs"):
        Path(output_dir).mkdir(exist_ok=True)

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath = Path(output_dir) / f"run_summary_{ts}.csv"

        headers = [
            "run_id", "run_started_at_utc",
            "ticker", "company_name",
            "cik", "accession", "filing_date",
            "status", "status_reason",
            "ex21_url", "ex21_embedded", "ex21_status", "ex21_subsidiaries", "ex21_http_status", "ex21_html_len",
            "ex211_url", "ex211_embedded", "ex211_status", "ex211_subsidiaries", "ex211_http_status", "ex211_html_len",
        ]

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for row in self.run_results:
                w.writerow({k: row.get(k, "") for k in headers})

        return str(filepath)

    def write_cik_not_found_csv(self, output_dir="run_logs"):
        Path(output_dir).mkdir(exist_ok=True)

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath = Path(output_dir) / f"cik_not_found_{ts}.csv"

        headers = ["run_id", "run_started_at_utc", "ticker", "company_name", "status_reason"]
        rows = [r for r in self.run_results if r.get("status_reason") == "CIK_NOT_FOUND"]

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in headers})

        return str(filepath)

    def write_exhibit_failures_csv(self, output_dir="run_logs", include_parsed_zero=True):
        """
        Filtered log CSV listing tickers where EX-21 and/or EX-21.1 is:
          - NOT_FOUND, ERROR, and optionally PARSED_ZERO
        """
        Path(output_dir).mkdir(exist_ok=True)

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath = Path(output_dir) / f"exhibit_failures_{ts}.csv"

        bad = {"NOT_FOUND", "ERROR"}
        if include_parsed_zero:
            bad.add("PARSED_ZERO")

        headers = [
            "run_id", "run_started_at_utc",
            "ticker", "company_name",
            "cik", "accession", "filing_date",
            "ex21_status", "ex21_subsidiaries", "ex21_http_status", "ex21_url", "ex21_embedded",
            "ex211_status", "ex211_subsidiaries", "ex211_http_status", "ex211_url", "ex211_embedded",
            "status", "status_reason",
        ]

        rows = []
        for r in self.run_results:
            if (r.get("ex21_status") in bad) or (r.get("ex211_status") in bad):
                rows.append(r)

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in headers})

        return str(filepath)

    # -------------------------------------------------------------------------
    # CORE: process a ticker (EX-21 + EX-21.1) and record results
    # -------------------------------------------------------------------------
    def process_ticker(self, ticker, company_name=None):
        if self.debug:
            print(f"\nProcessing: {ticker}")

        result = {
            "run_id": self.run_id,
            "run_started_at_utc": self.run_started_at,
            "ticker": ticker,
            "company_name": company_name or "",

            "cik": "",
            "accession": "",
            "filing_date": "",

            "status": "FAILED",
            "status_reason": "",

            "ex21_url": "",
            "ex21_embedded": "",
            "ex21_status": "NOT_FOUND",
            "ex21_subsidiaries": 0,
            "ex21_http_status": "",
            "ex21_html_len": "",

            "ex211_url": "",
            "ex211_embedded": "",
            "ex211_status": "NOT_FOUND",
            "ex211_subsidiaries": 0,
            "ex211_http_status": "",
            "ex211_html_len": "",
        }

        cik = self.get_cik(ticker)
        if not cik:
            result["status_reason"] = "CIK_NOT_FOUND"
            self.run_results.append(result)
            return None

        result["cik"] = cik

        meta = self.get_latest_filing(ticker, cik)
        if not meta:
            result["status_reason"] = "NO_10K_FOUND"
            self.run_results.append(result)
            return None

        result["accession"] = meta.get("accession", "")
        result["filing_date"] = meta.get("date", "")

        exhibits = self.get_exhibit_urls(meta)
        ex21 = exhibits.get("21")
        ex211 = exhibits.get("21.1")

        combined = []

        # EX-21
        if ex21:
            result["ex21_url"] = ex21["url"]
            result["ex21_embedded"] = str(bool(ex21["embedded"]))
            try:
                r = self.fetch_sec(ex21["url"], method="GET", timeout=45)
                result["ex21_http_status"] = r.status_code
                html = r.text or ""
                result["ex21_html_len"] = len(html)

                subs = self.extract_subsidiaries(html, is_embedded=ex21["embedded"], exhibit_label="21")
                result["ex21_subsidiaries"] = len(subs)
                result["ex21_status"] = "OK" if subs else "PARSED_ZERO"
                combined.extend(subs)
            except Exception as e:
                result["ex21_status"] = "ERROR"
                if not result["status_reason"]:
                    result["status_reason"] = f"EX21_FETCH_OR_PARSE_ERROR: {e}"

        # EX-21.1
        if ex211:
            result["ex211_url"] = ex211["url"]
            result["ex211_embedded"] = str(bool(ex211["embedded"]))
            try:
                r = self.fetch_sec(ex211["url"], method="GET", timeout=45)
                result["ex211_http_status"] = r.status_code
                html = r.text or ""
                result["ex211_html_len"] = len(html)

                subs = self.extract_subsidiaries(html, is_embedded=ex211["embedded"], exhibit_label="21.1")
                result["ex211_subsidiaries"] = len(subs)
                result["ex211_status"] = "OK" if subs else "PARSED_ZERO"
                combined.extend(subs)
            except Exception as e:
                result["ex211_status"] = "ERROR"
                if not result["status_reason"]:
                    result["status_reason"] = f"EX211_FETCH_OR_PARSE_ERROR: {e}"

        combined = self._deduplicate(combined)

        if combined:
            result["status"] = "OK"
            if not result["status_reason"]:
                result["status_reason"] = "SCRAPED_SUBSIDIARIES"
        else:
            if not result["status_reason"]:
                result["status_reason"] = "NO_SUBSIDIARIES_EXTRACTED"

        self.run_results.append(result)
        return combined

    # -------------------------------------------------------------------------
    # ZIP HELPERS (SPLIT OUTPUT)
    # -------------------------------------------------------------------------
    def _zip_files(self, zip_name, file_paths):
        with zipfile.ZipFile(zip_name, "w") as zf:
            for p in file_paths:
                zf.write(p, Path(p).name)

        if IN_COLAB:
            files.download(zip_name)
        else:
            print(f"Saved at: {Path(zip_name).absolute()}")

    # -------------------------------------------------------------------------
    # BATCH: from Google Sheet (TWO ZIPs)
    # -------------------------------------------------------------------------
    def process_from_sheet(self, sheet_url, sheet_name="Input"):
        self._new_run()

        if not self.connect_to_spreadsheet(sheet_url):
            return

        companies = self.read_company_list(sheet_name)

        subsidiaries_files = []
        log_files = []

        for co in companies:
            subs = self.process_ticker(co["ticker"], company_name=co.get("name"))
            if subs:
                csv_path = self.save_to_csv(subs, co, output_dir="subsidiary_csvs")
                if csv_path:
                    subsidiaries_files.append(csv_path)
            time.sleep(1)

        # Logs always produced
        summary_csv = self.write_run_summary_csv(output_dir="run_logs")
        cik_nf_csv = self.write_cik_not_found_csv(output_dir="run_logs")
        exhibit_fail_csv = self.write_exhibit_failures_csv(output_dir="run_logs", include_parsed_zero=True)
        log_files.extend([summary_csv, cik_nf_csv, exhibit_fail_csv])

        safe_sheet = self.safe_name(sheet_name, max_len=60)

        # 1) Subsidiaries zip (only if any subsidiaries CSVs exist)
        if subsidiaries_files:
            subs_zip = f"{safe_sheet}_Subsidiaries.zip"
            print(f"\nCreating subsidiaries ZIP: {subs_zip}")
            self._zip_files(subs_zip, subsidiaries_files)
        else:
            print("\nNo subsidiaries extracted; subsidiaries ZIP not generated.")

        # 2) Logs zip (always)
        logs_zip = f"{safe_sheet}_Logs.zip"
        print(f"\nCreating logs ZIP: {logs_zip}")
        self._zip_files(logs_zip, log_files)

    # -------------------------------------------------------------------------
    # SINGLE TICKER: (TWO ZIPs)
    # -------------------------------------------------------------------------
    def process_single_ticker(self, ticker):
        self._new_run()

        ref = self.parent_company_data.get(ticker, {})
        company_info = {
            "ticker": ticker,
            "name": ref.get("name") or ticker,
            "company_id": ref.get("company_id", ""),
        }

        subsidiaries_files = []
        log_files = []

        subs = self.process_ticker(ticker, company_name=company_info.get("name"))
        if subs:
            csv_path = self.save_to_csv(subs, company_info, output_dir="subsidiary_csvs")
            if csv_path:
                subsidiaries_files.append(csv_path)

        summary_csv = self.write_run_summary_csv(output_dir="run_logs")
        cik_nf_csv = self.write_cik_not_found_csv(output_dir="run_logs")
        exhibit_fail_csv = self.write_exhibit_failures_csv(output_dir="run_logs", include_parsed_zero=True)
        log_files.extend([summary_csv, cik_nf_csv, exhibit_fail_csv])

        safe_prefix = self.safe_name(ticker, max_len=20)

        if subsidiaries_files:
            subs_zip = f"{safe_prefix}_Subsidiaries.zip"
            print(f"\nCreating subsidiaries ZIP: {subs_zip}")
            self._zip_files(subs_zip, subsidiaries_files)
        else:
            print("\nNo subsidiaries extracted; subsidiaries ZIP not generated.")

        logs_zip = f"{safe_prefix}_Logs.zip"
        print(f"\nCreating logs ZIP: {logs_zip}")
        self._zip_files(logs_zip, log_files)


# -----------------------------------------------------------------------------
# STEP 4: USER INTERFACE
# -----------------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("SMART SUBSIDIARY SCRAPER (ULTIMATE EDITION v3.6)")
    print("=" * 60)

    scraper = SmartSubsidiaryScraper(debug=True)

    while True:
        print("\nOptions:")
        print("1. Process from Google Sheet (Batch) -> TWO ZIPs (Subsidiaries + Logs)")
        print("2. Process Single Ticker -> TWO ZIPs (Subsidiaries + Logs)")
        print("3. Exit")

        choice = input("\nEnter choice: ").strip()

        if choice == "1":
            url = input("Enter Google Sheet URL: ").strip()
            sheet_name = input("Enter Sheet Name (default 'Input'): ").strip() or "Input"
            scraper.process_from_sheet(url, sheet_name)

        elif choice == "2":
            ticker = input("Enter Ticker: ").strip().upper()
            scraper.process_single_ticker(ticker)

        elif choice == "3":
            break

if __name__ == "__main__":
    main()
