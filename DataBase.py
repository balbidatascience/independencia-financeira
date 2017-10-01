import pymysql

sqlCreateDimFI = "CREATE TABLE `independenciafinanceira`.`diminvestimento` ( " \
  "`idInvestimento` INT NOT NULL AUTO_INCREMENT, " \
  "`NomeInvestimento` VARCHAR(200) NOT NULL, " \
  "`Corretora` VARCHAR(200) NOT NULL, " \
  "`TipoInvestimento` VARCHAR(200) NOT NULL, " \
  "PRIMARY KEY (`idInvestimento`), " \
  "UNIQUE INDEX `NomeInvestimento_UNIQUE` (`NomeInvestimento` ASC));"

sqlCreateDimCalendario = "CREATE TABLE `independenciafinanceira`.`dimcalendario` ( " \
        "`idCalendario` INT UNSIGNED NOT NULL AUTO_INCREMENT, " \
        "`Data` DATE NOT NULL, " \
        "PRIMARY KEY (`idCalendario`), " \
        "UNIQUE INDEX `Data_UNIQUE` (`Data` ASC));"

#sqlCreateFatRendimento = CREATE TABLE `independenciafinanceira`.`fatrendimento` (
#  `idCalendario` INT NOT NULL,
#  `idInvestimento` INT NULL,
#  `ValorBruto` DECIMAL(9,2) NULL,
#  PRIMARY KEY (`idCalendario`));

#CREATE TABLE `independenciafinanceira`.`dsrendimentoinvestimento` (
#  `Data` DATE NULL,
#  `Investimento` VARCHAR(200) NULL,
#  `ValorBruto` DECIMAL(9,2) NULL

def getConn():
    return pymysql.connect(host='localhost', user='root', passwd='@gabriel', db='independenciafinanceira')

def getCursor(conn):
    return conn.cursor()

def createDataBase():
    conn = pymysql.connect(host='localhost', user='root', passwd='@gabriel', db='sys')
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA `independenciafinanceira`;")
    cur.close()
    conn.close()
    return True

def CreateDimFundoInvestimento():
    conn = pymysql.connect(host='localhost', user='root', passwd='@gabriel', db='independenciafinanceira')
    cur = conn.cursor()
    cur.execute(sqlCreateDimFI)
    cur.close()
    conn.close()
    return True

def CreateDimCalendario():
    conn = pymysql.connect(host='localhost', user='root', passwd='@gabriel', db='independenciafinanceira')
    cur = conn.cursor()
    cur.execute(sqlCreateDimCalendario)
    cur.close()
    conn.close()
    return True
