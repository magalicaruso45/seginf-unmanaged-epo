import ssl
import pyodbc
import MySQLdb
from keys import melicloud_db, epo_db,landesk_db

#para poder conectarme a epo sin ssl
try:
        _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
        # Legacy Python that doesn't verify HTTPS certificates by default
        pass
else:
        # Handle target environment that doesn't support HTTPS verification
        ssl._create_default_https_context = _create_unverified_https_context

def connect_to_landesk():
        try:
                conexion = MySQLdb.connect(host=landesk_db["server"],user=landesk_db["user"],passwd=landesk_db["pass"],db=landesk_db["database"],port=landesk_db["port"])
                # cnxn = pyodbc.connect('DRIVER={FreeTDS};SERVER=landesk_db["server"];PORT=landesk_db["port"];DATABASE=landesk_db["database"];UID=landesk_db["user"];PWD=landesk_db["pass"];Integrated Security=false;TDS_Version=7.2;CHARSET=UTF8')
                cursorLnd = cnxn.cursor()
                print "Se conecto a la base de landesk"
                return cursorLnd
        except Exception as e:
                print "No se conecto a la base de landesk " + str(e)

def connect_to_epo():
    cnxn = pyodbc.connect('DRIVER=FreeTDS;SERVER={0};PORT={1};DATABASE={2};UID={3};PWD={4};Integrated Security=false'.format(epo_db["host"],epo_db["port"],epo_db["db"],epo_db["user"],epo_db["password"]))
    cursor = cnxn.cursor()
    return cursor

def connect_to_hist():
    conexion = MySQLdb.connect(host=melicloud_db["host"],user=melicloud_db["user"],passwd=melicloud_db["password"],db=melicloud_db["db"],port=melicloud_db["port"])
    cursorHist = conexion.cursor()
    return cursorHist

def get_oficina(hostname):
    primerasLetras = hostname[:2]
    if primerasLetras == "BR":
        return 'MLB'
    elif primerasLetras == "AR":
        return 'MLA'
    elif primerasLetras == "UY":
        return 'MLU'
    elif primerasLetras == "CL":
        return 'MLC'
    elif primerasLetras == "CO":
        return 'MCO'
    elif primerasLetras == "VE":
        return 'MLV'
    elif primerasLetras == "MX":
        return 'MLM'

def listarUnmanaged(cursorEpo, cursorLnd, cursorHist):

    cursor.execute("select [EPOLeafNode].[AgentPlatform],[EPOLeafNode].[Tags], [EPOLeafNode].[NodeName], [EPOComputerProperties].[IPAddress] from [EPOLeafNode]  left join [EPOComputerProperties] on [EPOLeafNode].[AutoID] = [EPOComputerProperties].[ParentID] where [EPOLeafNode].[Tags] NOT LIKE '%aralvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%ararvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%arcbavcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%arslvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%arvlvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%brspvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%bvvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%clsgvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%cobovcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%dc_vm_auto%' and [EPOLeafNode].[Tags] NOT LIKE '%mxdfvcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%Server%' and [EPOLeafNode].[Tags] NOT LIKE '%usvivcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%uymovcenter01%' and [EPOLeafNode].[Tags] NOT LIKE '%vcentercourse%' and [EPOLeafNode].[Tags] NOT LIKE '%Vdi%'and [EPOLeafNode].[Tags] NOT LIKE '%vecavcenter01%'")
	rows = cursor.fetchall()
    contUnmanaged = 0
    contUnmanagedNoLandesk = 0
    contUnmanagedConLandesk = 0
    for system in systems:
        if system['EPOLeafNode.ManagedState'] == 0:
                hostname = system['EPOComputerProperties.ComputerName']
                iphostname = system['EPOComputerProperties.IPHostName']
                estaEnLandesk = consultaALandesk(hostname, cursorLnd)
                if not estaEnLandesk:
                    if hostname not in listaUnmanagedSinLandesk:
                        listaUnmanagedSinLandesk.append(hostname)
                        contUnmanagedNoLandesk += 1
                        query = "SELECT FROM `unmanaged` WHERE hostname = %s"
                        values = [hostname]
                        result = cursorHist.execute(query, values)
                        if not result:
                            print 'el host ' + hostname + ' no esta en la tabla de unmanaged'
                            oficina = get_oficina(hostname)
                            query = "INSERT INTO `unmanaged` (`hostname`,`ip`,`oficina`,`esta_en_landesk`) VALUES (%s,%s,%s,%s)"
                            values = [hostname,iphostname,oficina,0]
                            cursorHist.execute(query, values)
                else:
                    query = "SELECT FROM `unmanaged` WHERE hostname = %s"
                    values = [hostname]
                    result = cursorHist.execute(query, values)
                    if not result:
                        print 'el host ' + hostname + ' no esta en la tabla de unmanaged'
                        oficina = get_oficina(hostname)
                        query = "INSERT INTO `unmanaged` (`hostname`,`ip`,`oficina`,`esta_en_landesk`) VALUES (%s,%s,%s,%s)"
                        values = [hostname,iphostname,oficina,1]
                        cursorHist.execute(query, values)

                    if hostname not in listaUnmanagedConLandesk:
                        listaUnmanagedConLandesk.append(hostname)
                        contUnmanagedConLandesk += 1
                contUnmanaged += 1
        return listaUnmanagedConLandesk

def consultaALandesk(hostUnmanaged, cursorLnd):
        cursorLnd.execute("SELECT * FROM Computer WHERE DeviceName = '%s'" %(hostUnmanaged))
        rows = cursorLnd.fetchall()
        cont = 0
        for row in rows:
                hostname = str(row[4])
                if hostname == hostUnmanaged:
                        cont += 1
        return cont

cursorLnd = connect_to_landesk()
cursorHist = connect_to_hist()
cursorEpo = connect_to_epo()

if cursorLnd:
        unmanaged = listarUnmanaged(cursorEpo, cursorLnd, cursorHist)
