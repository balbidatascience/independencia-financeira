import pandas as pd
import pymysql
import DataBase as db
from datetime import datetime

sqlLoadFatRendimento = "insert into fatrendimento " \
                        "select 	c.idCalendario, " \
                        "        b.idInvestimento, ifnull(a.ValorBrutoMesAnterior, 0) as ValorMesAnterior,  " \
                        "        ifnull(a.ValorBruto, 0) as ValorBruto, ifnull(a.ValorAporte, 0) as ValorAporte, " \
                        "       ifnull(a.ValorRetirada, 0) as ValorRetirada, ifnull(a.ValorIR, 0) as ValorIR " \
                        "from dsrendimentoinvestimento a " \
                        "inner join diminvestimento b on a.Investimento = b.NomeInvestimento " \
                        "inner join dimcalendario c on a.Data = c.Data " \
                        "left join fatrendimento d on b.idInvestimento = d.idInvestimento " \
                        "                            and c.idCalendario = d.idCalendario " \
                        "where d.idCalendario is null"

sqlTruncateAll = "truncate table diminvestimento; " \
                 "truncate table fatrendimento; " \
                 "truncate table dsrendimentoinvestimento; " \
                 "truncate table dimcalendario;"

def getPlanilhaInvestimentos() :
    fi = pd.read_excel('dados\Investiment.xlsx', sheetname='FI')
    aportes = pd.read_excel('dados\Investiment.xlsx', sheetname='Aportes')
    acoes = pd.read_excel('dados\Investiment.xlsx', sheetname='Acoes')
    rendaFixa = pd.read_excel('dados\Investiment.xlsx', sheetname='Renda Fixa')
    fii = pd.read_excel('dados\Investiment.xlsx', sheetname='FII')
    return [fi, aportes, acoes, rendaFixa, fii]

def transformaFI(fi):
    investimento = pd.DataFrame(fi['FUNDO'].unique())
    investimento['Corretora'] = 'XP Investimentos'
    investimento['TipoInvestimento'] = 'Fundo de Investimento'
    investimento.columns = ['Nome FI', 'Corretora', 'TipoInvestimento']
    return investimento

def limpaCalendario(fi, aportes, acoes, rendaFixa, fii):
    calendario = fi['DATA'].append(rendaFixa['DATA']).append(aportes['Data'])
    calendario = pd.DataFrame(calendario.unique())
    calendario.columns = ['Data']
    calendario['Data'] = calendario['Data'].map(lambda x: datetime.date(x), na_action='ignore')
    return calendario

def tranformaRendimento(fi):
    rendimento = pd.DataFrame(fi[['DATA', 'FUNDO', 'Valor Mês Anterior', 'Valor Mês', 'Aporte no mês', 'Retiradas', 'IR']])
    rendimento['DATA'] = rendimento['DATA'].map(lambda x: datetime.date(x), na_action='ignore')
    rendimento = rendimento.fillna(0)
    return rendimento

def loadDimInvestimento(fi):
    conn = db.getConn()
    cur = db.getCursor(conn)
    query = 'INSERT INTO diminvestimento (NomeInvestimento, Corretora, TipoInvestimento) VALUES (%s, %s, %s)'
    cur.executemany(query, fi.values.tolist())
    conn.commit()

def loadDimCalendario(calendario):
    conn = db.getConn()
    cur = db.getCursor(conn)
    query = 'INSERT INTO dimcalendario (Data) VALUES (%s)'
    cur.executemany(query, calendario.values.tolist())
    conn.commit()

def loadDSRendimento(rendimento):
    conn = db.getConn()
    cur = db.getCursor(conn)
    query = 'INSERT INTO dsrendimentoinvestimento (Data, Investimento, ' \
            'ValorBrutoMesAnterior, ValorBruto, ' \
            'ValorAporte, ValorRetirada, ValorIR) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s)'
    cur.executemany(query, rendimento.values.tolist())
    conn.commit()

def loadFatRendimento():
    conn = db.getConn()
    cur = db.getCursor(conn)
    cur.execute(sqlLoadFatRendimento)
    conn.commit()

def clearDataBase():
    conn = db.getConn()
    cur = db.getCursor(conn)
    cur.execute(sqlTruncateAll)
    conn.commit()

def rodarETL():
    # Extracao
    investimentos = getPlanilhaInvestimentos()
    fi = investimentos[0]
    aportes = investimentos[1]
    acoes = investimentos[2]
    rendaFixa = investimentos[3]
    fii = investimentos[4]

    # Transforma
    investimento = transformaFI(fi)
    calendario = limpaCalendario(fi=fi, aportes=aportes, rendaFixa=rendaFixa, fii=fii, acoes=acoes)
    rendimento = tranformaRendimento(fi)

    # Clear all
    clearDataBase()

    # Load
    loadDimInvestimento(investimento)
    loadDimCalendario(calendario)
    loadDSRendimento(rendimento)
    loadFatRendimento()


#-----------------------------------
# Testes
#-----------------------------------

def testLoadDimInvestimento():
    # Carregando dados
    investimentos = getPlanilhaInvestimentos()
    fi = investimentos[0]
    aportes = investimentos[1]
    acoes = investimentos[2]
    rendaFixa = investimentos[3]
    fii = investimentos[4]

    # Limpeza
    fi = transformaFI(fi)

    # Load
    loadDimInvestimento(fi)

def testLoadTempo():
    investimentos = getPlanilhaInvestimentos()
    fi = investimentos[0]
    aportes = investimentos[1]
    acoes = investimentos[2]
    rendaFixa = investimentos[3]
    fii = investimentos[4]

    #Limpeza
    calendario = limpaCalendario(fi=fi, aportes=aportes, rendaFixa=rendaFixa, fii=fii, acoes=acoes)
    #print(type(calendario['Data'][0]))
    #Load
    loadDimCalendario(calendario)

def testLoadDsRendimento():
    investimentos = getPlanilhaInvestimentos()
    fi = investimentos[0]
    aportes = investimentos[1]
    acoes = investimentos[2]
    rendaFixa = investimentos[3]
    fii = investimentos[4]

    rendimento = tranformaRendimento(fi)
    loadDSRendimento(rendimento)

#testLoadDimInvestimento()
#loadFatRendimento()

rodarETL()