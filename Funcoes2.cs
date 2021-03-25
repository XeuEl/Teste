using Sap.Data.SQLAnywhere;
using System;
using System.Configuration;
using System.Data.SqlClient;

namespace Sybase_SQL
{

    class Funcoes2
    {
        static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        SqlConnection SQLcnn, SQLcnnToytaL1, SQLcnnToytaL2, SQLcnnToytaL3;
        SAConnection SYBASEConn_DTestIL1 = null, SYBASEConn_DTestIL2 = null, SYBASEConn_DTestIL3 = null;
        public int contador;
        public bool temErro = false;
        //usado apenas para teste
        bool gravaLocal = false, InserirDados = true;


        /// <summary>
        /// Retorna status da função 
        /// </summary>
        /// <param name="arg">Código do status</param>
        /// <returns>Status da função</returns>
        public string getStatus(int arg)
        {
            switch (arg)
            {
                case (int)Status.Erro:
                    return "Erro! Verificar log";
                case (int)Status.Aguardando:
                    return "Em tempo de espera";
                case (int)Status.DadosOP:
                    return "Pesquisando dados da OP";
                case (int)Status.GravandoTabelas:
                    return "Gravando Tabelas";
                case (int)Status.DadosSincronizados:
                    return "Dados sincronizados com sucesso";
                case (int)Status.SemNovaOP:
                    return "Sem dados novos";
                case (int)Status.SincronizandoToyota:
                    return "Sincronizando dados do Toyota";
                default:
                    return "";
            }
        }

        public void escreveLog(int arg, string arg2)
        {
            if (arg == 1)
            {
                log.Info(arg2);
            }
            if (arg == 2)
            {
                log.Error(arg2);
            }
        }

        public int getTimerValor(string timer)
        {
            int tempoout;
            if (int.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("timer_L" + timer), out tempoout))
            {
                return tempoout;
            }
            else
            {
                return 120000;
            }
        }

        public void initConexaoSQL()
        {
            if (gravaLocal)
            {
                SQLcnn = new SqlConnection("Server=.\\SQLExpress;Database=sybase_linhas;User Id=admta;Password=admta;");
                SQLcnn.Open();
                log.Info("Database SQL Express 2008 connectado - EM LOCAL");
                return;
            }

            try
            {
                if (SQLcnn != null)
                {
                    if (SQLcnn.State != System.Data.ConnectionState.Open)
                    {
                        try
                        {
                            SQLcnn.Close();
                        }
                        catch (Exception) { SQLcnn = null; }

                        try
                        {
                            SQLcnn.Dispose();
                        }
                        catch (Exception) { SQLcnn = null; }

                        SQLcnn = null;

                        SQLcnn = new SqlConnection(ConfigurationManager.AppSettings.Get("Str_Con_SQLE"));
                        SQLcnn.Open();

                        log.Info("Database SQL Express 2008 connectado");
                    }
                }
                else
                {
                    SQLcnn = new SqlConnection(ConfigurationManager.AppSettings.Get("Str_Con_SQLE"));
                    SQLcnn.Open();

                    log.Info("Database SQL Express 2008 connectado");
                }
            }
            catch (Exception e)
            {
                log.Error("initConexaoSQL: " + e.Message);
            }
        }

        public void initConexaoSQLToyota(int linha)
        {
            if (gravaLocal) { return; }

            try
            {
                string Lcnn = ConfigurationManager.AppSettings.Get("Str_Con_ToytaL" + linha.ToString());

                switch (linha)
                {
                    case 1:
                        if (SQLcnnToytaL1 != null)
                        {
                            if (SQLcnnToytaL1.State != System.Data.ConnectionState.Open)
                            {
                                try
                                {
                                    SQLcnnToytaL1.Close();
                                }
                                catch (Exception) { SQLcnnToytaL1 = null; }

                                try
                                {
                                    SQLcnnToytaL1.Dispose();
                                }
                                catch (Exception) { SQLcnnToytaL1 = null; }

                                SQLcnnToytaL1 = null;

                                SQLcnnToytaL1 = new SqlConnection(Lcnn);
                                SQLcnnToytaL1.Open();

                                log.Info("Database SQL Toyota linha " + linha.ToString() + " connectado");
                            }
                        }
                        else
                        {
                            SQLcnnToytaL1 = new SqlConnection(Lcnn);
                            SQLcnnToytaL1.Open();

                            log.Info("Database SQL Toyota linha " + linha.ToString() + " connectado");
                        }
                        break;
                    case 2:
                        if (SQLcnnToytaL2 != null)
                        {
                            if (SQLcnnToytaL2.State != System.Data.ConnectionState.Open)
                            {
                                try
                                {
                                    SQLcnnToytaL2.Close();
                                }
                                catch (Exception) { SQLcnnToytaL2 = null; }

                                try
                                {
                                    SQLcnnToytaL2.Dispose();
                                }
                                catch (Exception) { SQLcnnToytaL2 = null; }

                                SQLcnnToytaL2 = null;

                                SQLcnnToytaL2 = new SqlConnection(Lcnn);
                                SQLcnnToytaL2.Open();

                                log.Info("Database SQL Toyota linha " + linha.ToString() + " connectado");
                            }
                        }
                        else
                        {
                            SQLcnnToytaL2 = new SqlConnection(Lcnn);
                            SQLcnnToytaL2.Open();

                            log.Info("Database SQL Toyota linha " + linha.ToString() + " connectado");
                        }
                        break;
                    case 3:
                        if (SQLcnnToytaL3 != null)
                        {
                            if (SQLcnnToytaL3.State != System.Data.ConnectionState.Open)
                            {
                                try
                                {
                                    SQLcnnToytaL3.Close();
                                }
                                catch (Exception) { SQLcnnToytaL3 = null; }

                                try
                                {
                                    SQLcnnToytaL3.Dispose();
                                }
                                catch (Exception) { SQLcnnToytaL3 = null; }

                                SQLcnnToytaL3 = null;

                                SQLcnnToytaL3 = new SqlConnection(Lcnn);
                                SQLcnnToytaL3.Open();

                                log.Info("Database SQL Toyota linha " + linha.ToString() + " connectado");
                            }
                        }
                        else
                        {
                            SQLcnnToytaL3 = new SqlConnection(Lcnn);
                            SQLcnnToytaL3.Open();

                            log.Info("Database SQL Toyota linha " + linha.ToString() + " connectado");
                        }
                        break;
                }
            }
            catch (Exception e)
            {
                log.Error("initConexaoSQLToyota(" + linha.ToString() + "): " + e.Message);
            }
        }

        /// <summary>
        /// Inicializa a conexão com o banco de dados Sybase
        /// </summary>
        /// <param name="arg">Linha</param>
        public void initConexaoSybase(int arg)
        {
            if (gravaLocal)
            {
                SAConnectionStringBuilder sb2 = new SAConnectionStringBuilder();
                sb2.UserID = "result";
                sb2.Password = "result";
                sb2.ServerName = "DTestI";


                SYBASEConn_DTestIL1 = new SAConnection(sb2.ConnectionString);
                SYBASEConn_DTestIL1.Open();

                log.Info("Database Sybase linha " + arg.ToString() + " connectado - EM LOCAL");
                return;
            }


            try
            {
                SAConnectionStringBuilder sb = new SAConnectionStringBuilder();
                sb.UserID = "result";
                sb.Password = "result";
                sb.ServerName = "DTestI";
                sb.Host = ConfigurationManager.AppSettings.Get("IP_Sybase_L" + arg.ToString()) + ":" + ConfigurationManager.AppSettings.Get("Port_Sybase_L" + arg.ToString());
                sb.DatabaseName = "result";

                switch (arg)
                {
                    case 1:
                        if (SYBASEConn_DTestIL1 != null)
                        {
                            if (SYBASEConn_DTestIL1.State != System.Data.ConnectionState.Open)
                            {
                                try
                                {
                                    SYBASEConn_DTestIL1.Close();
                                }
                                catch (Exception) { SYBASEConn_DTestIL1 = null; }

                                try
                                {
                                    SYBASEConn_DTestIL1.Dispose();
                                }
                                catch (Exception) { SYBASEConn_DTestIL1 = null; }

                                SYBASEConn_DTestIL1 = null;

                                SAConnectionStringBuilder sb2 = new SAConnectionStringBuilder();
                                sb2.UserID = "result";
                                sb2.Password = "result";
                                sb2.ServerName = "DTestI";


                                SYBASEConn_DTestIL1 = new SAConnection(sb.ConnectionString);
                                SYBASEConn_DTestIL1.Open();

                                log.Info("Database Sybase linha " + arg.ToString() + " connectado");
                            }
                        }
                        else
                        {

                            SAConnectionStringBuilder sb2 = new SAConnectionStringBuilder();
                            sb2.UserID = "result";
                            sb2.Password = "result";
                            sb2.ServerName = "DTestI";


                            SYBASEConn_DTestIL1 = new SAConnection(sb.ConnectionString);
                            SYBASEConn_DTestIL1.Open();
                            log.Info("Database Sybase linha " + arg.ToString() + " connectado");
                        }

                        break;
                    case 2:
                        if (SYBASEConn_DTestIL2 != null)
                        {
                            if (SYBASEConn_DTestIL2.State != System.Data.ConnectionState.Open)
                            {
                                try
                                {
                                    SYBASEConn_DTestIL2.Close();
                                }
                                catch (Exception) { SYBASEConn_DTestIL2 = null; }

                                try
                                {
                                    SYBASEConn_DTestIL2.Dispose();
                                }
                                catch (Exception) { SYBASEConn_DTestIL2 = null; }

                                SYBASEConn_DTestIL2 = null;

                                SYBASEConn_DTestIL2 = new SAConnection(sb.ConnectionString);
                                SYBASEConn_DTestIL2.Open();

                                log.Info("Database Sybase linha " + arg.ToString() + " connectado");
                            }
                        }
                        else
                        {
                            SYBASEConn_DTestIL2 = new SAConnection(sb.ConnectionString);
                            SYBASEConn_DTestIL2.Open();
                            log.Info("Database Sybase linha " + arg.ToString() + " connectado");
                        }

                        break;
                    case 3:
                        if (SYBASEConn_DTestIL3 != null)
                        {
                            if (SYBASEConn_DTestIL3.State != System.Data.ConnectionState.Open)
                            {
                                try
                                {
                                    SYBASEConn_DTestIL3.Close();
                                }
                                catch (Exception) { SYBASEConn_DTestIL3 = null; }

                                try
                                {
                                    SYBASEConn_DTestIL3.Dispose();
                                }
                                catch (Exception) { SYBASEConn_DTestIL3 = null; }

                                SYBASEConn_DTestIL3 = null;

                                SYBASEConn_DTestIL3 = new SAConnection(sb.ConnectionString);
                                SYBASEConn_DTestIL3.Open();

                                log.Info("Database Sybase linha " + arg.ToString() + " connectado");
                            }
                        }
                        else
                        {
                            SYBASEConn_DTestIL3 = new SAConnection(sb.ConnectionString);
                            SYBASEConn_DTestIL3.Open();
                            log.Info("Database Sybase linha " + arg.ToString() + " connectado");
                        }

                        break;
                }
            }
            catch (Exception e)
            {
                log.Error("initConexaoSybaseL" + arg.ToString() + ": " + e.Message);
            }
        }

        public void gravaDados(int linha)
        {
            try
            {
                SACommand SybaseCmd = new SACommand();
                SADataReader SyabaseDR;

                SqlCommand SQLcmd = new SqlCommand();
                SqlDataReader SQLDR;

                switch (linha)
                {
                    case 1:
                        SybaseCmd.Connection = SYBASEConn_DTestIL1;
                        break;
                    case 2:
                        SybaseCmd.Connection = SYBASEConn_DTestIL2;
                        break;
                    case 3:
                        SybaseCmd.Connection = SYBASEConn_DTestIL3;
                        break;
                }

                SQLcmd.Connection = SQLcnn;

                string sql, dateConvert;
                contador = 0;
                temErro = false;

                //Variaveis usadas para gravar dados geral
                string OPAntiga = "";
                int numeroBarra, PieceNoStart = 0, PieceNoEnd = 0;
                bool primeiraBarra = true, primeiraLinha = false;
                DadosOP _dadosOP = new DadosOP();
                System.Collections.Generic.List<DadosOP> OPsList = new System.Collections.Generic.List<DadosOP>();

                if (!gravaLocal)
                {
                    try
                    {
                        #region GRAVA TOYOTA
                        SqlCommand SQLCmdT = new SqlCommand();

                        string tabelaNome = "", temMag = "", colunaDesponte = "", mag = "", order = "";
                        int offset = 0;

                        switch (linha)
                        {
                            case 1:
                                SQLCmdT.Connection = SQLcnnToytaL1;
                                tabelaNome = "[sqo_BarraDefeitos_MAG]";
                                colunaDesponte = "[desponte]";
                                mag = "1";
                                offset = 1;
                                order = "[data]";
                                break;
                            case 2:
                                SQLCmdT.Connection = SQLcnnToytaL2;
                                tabelaNome = "[sqo_BarraDefeitos_MAG]";
                                colunaDesponte = "[desponte]";
                                mag = "2";
                                offset = 1;
                                order = "[id]";
                                break;
                            case 3:
                                SQLCmdT.Connection = SQLcnnToytaL3;
                                tabelaNome = "[super]";
                                colunaDesponte = "[despontado]";
                                temMag = "[MAG],";
                                order = "[id]";
                                break;
                        }


                        SQLCmdT.CommandText = "SELECT top(500) [barraOP],[barraID],[defeitoID],[defeitoTipo],[acao],[origem],[data],[operador]," + temMag + " " + colunaDesponte + ",[bitolaReal] FROM [dbo]." + tabelaNome + " ORDER BY " + order + " DESC";                      

                        SQLDR = SQLCmdT.ExecuteReader();

                        while (SQLDR.Read())
                        {

                            if (linha == 3)
                            {
                                mag = SQLDR.GetValue(8).ToString();
                            }

                            if (SQLDR.GetValue(6).ToString().Contains("M"))
                            {
                                dateConvert = "101";
                            }
                            else
                            {
                                dateConvert = "103";
                            }

                            if (linha == 2)
                            {
                                dateConvert = "20";
                            }

                            sql = "INSERT INTO[dbo].[toyota]" +
                                  "([barraOP]" +
                                  ",[barraID]" +
                                  ",[defeitoID]" +
                                  ",[defeitoTipo]" +
                                  ",[acao]" +
                                  ",[origem]" +
                                  ",[data]" +
                                  ",[operador]" +
                                  ",[MAG]" +
                                  ",[despontado]" +
                                  ",[bitolaReal]" +
                                  ",[linha])" +
                            " SELECT  " +
                                  " '" + SQLDR.GetValue(0).ToString() + "' " +
                                  ", '" + SQLDR.GetValue(1).ToString() + "'" +
                                  ", '" + SQLDR.GetValue(2).ToString() + "'" +
                                  ", '" + SQLDR.GetValue(3).ToString() + "'" +
                                  ", '" + SQLDR.GetValue(4).ToString() + "'" +
                                  ", '" + SQLDR.GetValue(5).ToString() + "'" +
                                  ", CONVERT(DATETIME, '" + SQLDR.GetValue(6).ToString() + "', " + dateConvert + ")" +
                                  ", '" + SQLDR.GetValue(7).ToString() + "'" +
                                  ", '" + mag + "'" +
                                  ", '" + SQLDR.GetValue(9 - offset).ToString() + "'" +
                                  ", '" + SQLDR.GetValue(10 - offset).ToString() + "'" +
                                  ", " + linha.ToString() + " WHERE NOT EXISTS  " +
                                  " (SELECT [barraOP] from [toyota] WHERE [barraOP] = '" + SQLDR.GetValue(0).ToString() + "' AND [barraID] = '" + SQLDR.GetValue(1).ToString() + "' AND [defeitoID] = '" + SQLDR.GetValue(2).ToString() + "' AND [linha] = '" + linha.ToString() + "') ";

                            SQLcmd = new SqlCommand(sql, SQLcnn);
                            if (InserirDados) { contador += SQLcmd.ExecuteNonQuery(); }

                        }

                        Console.WriteLine("gravar tabela Toyota (LINHAS): " + contador.ToString());

                        SQLDR.Close();
                        SQLCmdT.Dispose();
                        #endregion
                    }
                    catch (Exception e)
                    {
                        temErro = true;
                        log.Error("Erro ao gravar tabela Toyota: " + e.Message);
                    }
                }

                try
                {
                    #region GRAVA PIECE
                    SybaseCmd.CommandText = "SELECT TOP(1000) [pieceNo], [requestNo], [parDocNo], [parDynNo], [parStaNo], [pieceIdent], [pieceTestTime], [pieceLength], [evaluationMethod], [pieceValuation], [pieceValuationText], [detailUnsave], [transport], [externalSave] FROM \"result\".\"piece\" ORDER BY [pieceNo] DESC ";
                    SyabaseDR = SybaseCmd.ExecuteReader();

                    primeiraLinha = true;
                    while (SyabaseDR.Read())
                    {

                        //Organiza dados para gravar dados geral
                        //inicio
                        if (OPsList.Count < 4)
                        {
                            if (primeiraBarra)
                            {
                                OPAntiga = SyabaseDR.GetValue(5).ToString().Split(' ')[0];
                                primeiraBarra = false;
                            }

                            int.TryParse(SyabaseDR.GetValue(5).ToString().Split(' ')[1], out numeroBarra);
                            if (numeroBarra == 1)
                            {
                                if (OPAntiga.Equals(SyabaseDR.GetValue(5).ToString().Split(' ')[0]))
                                {
                                    PieceNoStart = int.Parse(SyabaseDR.GetValue(0).ToString());

                                    _dadosOP.pieceNoStart = SyabaseDR.GetValue(0).ToString();
                                    _dadosOP.OP = OPAntiga;

                                    OPsList.Add(_dadosOP);

                                    _dadosOP = new DadosOP();
                                }
                            }

                            if (!(OPAntiga.Equals(SyabaseDR.GetValue(5).ToString().Split(' ')[0])))
                            {
                                PieceNoEnd = int.Parse(SyabaseDR.GetValue(0).ToString());

                                _dadosOP.pieceNoEnd = SyabaseDR.GetValue(0).ToString();

                                OPAntiga = SyabaseDR.GetValue(5).ToString().Split(' ')[0];
                            }
                        }
                        //fim

                        if (SyabaseDR.GetValue(6).ToString().Contains("M"))
                        {
                            dateConvert = "101";
                        }
                        else
                        {
                            dateConvert = "103";
                        }

                        sql = "INSERT INTO [dbo].[piece]" +
                              "([pieceNo]" +
                              ",[requestNo]" +
                              ",[parDocNo]" +
                              ",[parDynNo]" +
                              ",[parStaNo]" +
                              ",[pieceIdent]" +
                              ",[pieceTestTime]" +
                              ",[pieceLength]" +
                              ",[evaluationMethod]" +
                              ",[pieceValuation]" +
                              ",[pieceValuationText]" +
                              ",[detailUnsave]" +
                              ",[transport]" +
                              ",[externalSave]" +
                              ",[linha])" +
                        " SELECT " +
                              " '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString().Replace("'", "''").Trim() + "'" +
                              ", CONVERT(DATETIME, '" + SyabaseDR.GetValue(6).ToString() + "', " + dateConvert + ")" +
                              ", '" + SyabaseDR.GetValue(7).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(8).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(9).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(10).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(11).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(12).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(13).ToString().Replace("'", "''") + "'" +
                              "," + linha.ToString() + " WHERE NOT EXISTS  " +
                              " (SELECT [pieceNo] from [piece] WHERE [pieceNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [linha]='" + linha.ToString() + "') ";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados)
                        {
                            if (primeiraLinha)
                            {
                                primeiraLinha = false;
                            } else
                            {
                                contador += SQLcmd.ExecuteNonQuery();
                            }       
                        }
                    }

                    Console.WriteLine("gravar tabela Piece (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela Piece: " + e.Message);
                }

                try
                {
                    #region GRAVA DEFEITO
                    SybaseCmd.CommandText = "SELECT TOP(2000) defectNo, pieceNo, deviceNo, defectPosition, defectExtension, defectClass, defectAmplitude, eventLineNo, defectValuation, defectAddInfo1, defectAddInfo2, defectAddInfo3, defectCode FROM \"result\".\"defect\" ORDER BY defectNo DESC";
                    SyabaseDR = SybaseCmd.ExecuteReader();

                    while (SyabaseDR.Read())
                    {
                        sql = "INSERT INTO[dbo].[defect]" +
                              "([defectNo]" +
                              ",[pieceNo]" +
                              ",[deviceNo]" +
                              ",[defectPosition]" +
                              ",[defectExtension]" +
                              ",[defectClass]" +
                              ",[defectAmplitude]" +
                              ",[eventLineNo]" +
                              ",[defectValuation]" +
                              ",[defectAddInfo1]" +
                              ",[defectAddInfo2]" +
                              ",[defectAddInfo3]" +
                              ",[defectCode]" +
                              ",[linha])" +
                        " SELECT " +
                              "  '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(6).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(7).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(8).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(9).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(10).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(11).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(12).ToString().Replace("'", "''") + "'" +
                              ", " + linha.ToString() + " WHERE NOT EXISTS " +
                              " (SELECT [defectNo] from [defect] WHERE [defectNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [linha] = '" + linha.ToString() + "') ";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados) { contador += SQLcmd.ExecuteNonQuery(); }

                    }

                    Console.WriteLine("gravar tabela defeito (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela defeito: " + e.Message);
                }

                try
                {
                    #region GRAVA PARDOC
                    SybaseCmd.CommandText = "SELECT TOP(100) [parNo], [parOrder], [parRef], [deviceNo], [systemNo], [sensorNo], [modulTypeIdent], [parIdent], [parDimIdent], [parVal] FROM \"result\".\"parDoc\" ORDER BY [parNo] DESC";
                    SyabaseDR = SybaseCmd.ExecuteReader();


                    while (SyabaseDR.Read())
                    {
                        sql = "INSERT INTO[dbo].[parDoc]" +
                              "([parNo]" +
                              ",[parOrder]" +
                              ",[parRef]" +
                              ",[deviceNo]" +
                              ",[systemNo]" +
                              ",[sensorNo]" +
                              ",[modulTypeIdent]" +
                              ",[parIdent]" +
                              ",[parDimIdent]" +
                              ",[parVal]" +
                              ",[linha])" +
                        " SELECT " +
                              "  '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'  " +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'  " +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'  " +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'  " +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'  " +
                              ", '" + SyabaseDR.GetValue(6).ToString().Replace("'", "''") + "'  " +
                              ", '" + SyabaseDR.GetValue(7).ToString().Replace("'", "''") + "'  " +
                              ", '" + SyabaseDR.GetValue(8).ToString().Replace("'", "''") + "'  " +
                              ", '" + SyabaseDR.GetValue(9).ToString().Replace("'", "''") + "'  " +
                              "," + linha.ToString() + " WHERE NOT EXISTS " +
                               "(SELECT [parNo] from [parDoc] WHERE [parNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [linha] = '" + linha.ToString() + "') ";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados) { contador += SQLcmd.ExecuteNonQuery(); }

                    }

                    Console.WriteLine("gravar tabela ParDoc (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela ParDoc: " + e.Message);
                }

                try
                {
                    #region GRAVA PARDYN
                    SybaseCmd.CommandText = "SELECT TOP(100) [parNo], [parOrder], [parRef], [deviceNo], [systemNo], [sensorNo], [modulTypeIdent], [parIdent], [parDimIdent], [parVal] FROM \"result\".\"parDyn\" ORDER BY [parNo] DESC ";
                    SyabaseDR = SybaseCmd.ExecuteReader();

                    while (SyabaseDR.Read())
                    {
                        sql = "INSERT INTO[dbo].[parDyn]" +
                              "([parNo]" +
                              ",[parOrder]" +
                              ",[parRef]" +
                              ",[deviceNo]" +
                              ",[systemNo]" +
                              ",[sensorNo]" +
                              ",[modulTypeIdent]" +
                              ",[parIdent]" +
                              ",[parDimIdent]" +
                              ",[parVal]" +
                              ",[linha])" +
                        " SELECT " +
                              " '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(6).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(7).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(8).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(9).ToString().Replace("'", "''") + "'" +
                              "," + linha.ToString() + " WHERE NOT EXISTS " +
                              "(SELECT [parNo] from [parDyn] WHERE [parOrder] = '" + SyabaseDR.GetValue(1).ToString() + "' AND [parNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [linha]='" + linha.ToString() + "')";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados) { contador += SQLcmd.ExecuteNonQuery(); }

                    }

                    Console.WriteLine("gravar tabela ParDyn (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela ParDyn: " + e.Message);
                }

                try
                {
                    #region GRAVA PARSTA
                    SybaseCmd.CommandText = "SELECT TOP(100) [parNo], [parOrder], [parRef], [deviceNo], [systemNo], [sensorNo], [modulTypeIdent], [parIdent], [parDimIdent], [parVal] FROM \"result\".\"parSta\" ORDER BY [parNo] DESC";
                    SyabaseDR = SybaseCmd.ExecuteReader();

                    while (SyabaseDR.Read())
                    {
                        sql = "INSERT INTO[dbo].[parSta]" +
                              "([parNo]" +
                              ",[parOrder]" +
                              ",[parRef]" +
                              ",[deviceNo]" +
                              ",[systemNo]" +
                              ",[sensorNo]" +
                              ",[modulTypeIdent]" +
                              ",[parIdent]" +
                              ",[parDimIdent]" +
                              ",[parVal]" +
                              ",[linha])" +
                        " SELECT " +
                              " '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(6).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(7).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(8).ToString().Replace("'", "''") + "'" +
                              ", '" + SyabaseDR.GetValue(9).ToString().Replace("'", "''") + "'" +
                              ", " + linha.ToString() + " WHERE NOT EXISTS " +
                              "(SELECT [parNo] from [parSta] WHERE [parNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [parOrder] = '" + SyabaseDR.GetValue(1).ToString() + "' AND [linha]='" + linha.ToString() + "')";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados) { contador += SQLcmd.ExecuteNonQuery(); }

                    }

                    Console.WriteLine("gravar tabela ParSta (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela ParSta: " + e.Message);
                }

                try
                {
                    #region GRAVA PIECEDETAIL
                    SybaseCmd.CommandText = "SELECT TOP(1000) [pieceDetailNo],[pieceNo],[deviceNo],[parDocNo],[parDynNo],[parStaNo],[pieceTestTime],[pieceLength],[evaluationMethod],[pieceTestResult1]" +
                            ",[pieceTestResult2],[pieceTestResult3],[pieceTestResult4],[pieceTestResult5],[pieceTestResult6],[pieceValuation],[detailUnsave],[requestNo]" +
                            "FROM \"result\".\"pieceDetail\" ORDER BY [pieceDetailNo] DESC ";
                    SyabaseDR = SybaseCmd.ExecuteReader();

                    primeiraBarra = true;
                    while (SyabaseDR.Read())
                    {
                        if (SyabaseDR.GetValue(6).ToString().Contains("M"))
                        {
                            dateConvert = "101";
                        }
                        else
                        {
                            dateConvert = "103";
                        }

                        sql = "INSERT INTO[dbo].[pieceDetail]" +
                              "([pieceDetailNo]" +
                              ",[pieceNo]" +
                              ",[deviceNo]" +
                              ",[parDocNo]" +
                              ",[parDynNo]" +
                              ",[parStaNo]" +
                              ",[pieceTestTime]" +
                              ",[pieceLength]" +
                              ",[evaluationMethod]" +
                              ",[pieceTestResult1]" +
                              ",[pieceTestResult2]" +
                              ",[pieceTestResult3]" +
                              ",[pieceTestResult4]" +
                              ",[pieceTestResult5]" +
                              ",[pieceTestResult6]" +
                              ",[pieceValuation]" +
                              ",[detailUnsave]" +
                              ",[requestNo]" +
                              ",[linha])" +
                        " SELECT " +
                              " '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'" +
                              ", CONVERT(DATETIME, '" + SyabaseDR.GetValue(6).ToString() + "', " + dateConvert + ")" +
                              ", '" + SyabaseDR.GetValue(7).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(8).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(9).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(10).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(11).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(12).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(13).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(14).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(15).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(16).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(17).ToString() + "'" +
                              "," + linha.ToString() + " WHERE NOT EXISTS " +
                              "(SELECT [pieceDetailNo] from [pieceDetail] WHERE [pieceDetailNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [linha]='" + linha.ToString() + "')";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados)
                        {
                            if (primeiraLinha)
                            {
                                primeiraLinha = false;
                            }
                            else
                            {
                                contador += SQLcmd.ExecuteNonQuery();
                            }
                        }

                    }
                    Console.WriteLine("gravar tabela PieceDetail (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela PieceDetail: " + e.Message);
                }

                try
                {
                    #region GRAVA REQUEST
                    SybaseCmd.CommandText = "SELECT top(100) [requestNo],[requestName],[requestStart],[requestEnd],[pieceName],[pieceNumber],[pieceNameRef],[pieceNumberRef],[remark01name],[remark01value],[remark02name]" +
                            ",[remark02value],[remark03name],[remark03value],[remark04name],[remark04value],[remark05name],[remark05value],[remark06name],[remark06value]" +
                            ",[remark07name],[remark07value],[remark08name],[remark08value],[remark09name],[remark09value],[remark10name],[remark10value],[remark11name]" +
                            ",[remark11value],[remark12name],[remark12value],[remark13name],[remark13value],[remark14name],[remark14value],[remark15name],[remark15value]" +
                            ",[remark16name],[remark16value],[remark17name],[remark17value],[remark18name],[remark18value],[remark19name],[remark19value],[remark20name],[remark20value],[externalSave] FROM \"result\".\"request\" ORDER BY [requestNo] DESC";
                    SyabaseDR = SybaseCmd.ExecuteReader();

                    primeiraBarra = true;
                    while (SyabaseDR.Read())
                    {
                        //dados para dados geral
                        //inicio
                        for (int i = 0; i < OPsList.Count; i++)
                        {
                            if (OPsList[i].OP.Equals(SyabaseDR.GetValue(1).ToString()))
                            {
                                OPsList[i].requestNo = SyabaseDR.GetValue(0).ToString();
                                OPsList[i].requestStart = SyabaseDR.GetValue(2).ToString();
                            }
                        }
                        //fim

                        if (SyabaseDR.GetValue(2).ToString().Contains("M"))
                        {
                            dateConvert = "101";
                        }
                        else
                        {
                            dateConvert = "103";
                        }

                        sql = "INSERT INTO [dbo].[request]" +
                                                  "([requestNo]" +
                                                  ",[requestName]" +
                                                  ",[requestStart]" +
                                                  ",[requestEnd]" +
                                                  ",[pieceName]" +
                                                  ",[pieceNumber]" +
                                                  ",[pieceNameRef]" +
                                                  ",[pieceNumberRef]" +
                                                  ",[remark01name]" +
                                                  ",[remark01value]" +
                                                  ",[remark02name]" +
                                                  ",[remark02value]" +
                                                  ",[remark03name]" +
                                                  ",[remark03value]" +
                                                  ",[remark04name]" +
                                                  ",[remark04value]" +
                                                  ",[remark05name]" +
                                                  ",[remark05value]" +
                                                  ",[remark06name]" +
                                                  ",[remark06value]" +
                                                  ",[remark07name]" +
                                                  ",[remark07value]" +
                                                  ",[remark08name]" +
                                                  ",[remark08value]" +
                                                  ",[remark09name]" +
                                                  ",[remark09value]" +
                                                  ",[remark10name]" +
                                                  ",[remark10value]" +
                                                  ",[remark11name]" +
                                                  ",[remark11value]" +
                                                  ",[remark12name]" +
                                                  ",[remark12value]" +
                                                  ",[remark13name]" +
                                                  ",[remark13value]" +
                                                  ",[remark14name]" +
                                                  ",[remark14value]" +
                                                  ",[remark15name]" +
                                                  ",[remark15value]" +
                                                  ",[remark16name]" +
                                                  ",[remark16value]" +
                                                  ",[remark17name]" +
                                                  ",[remark17value]" +
                                                  ",[remark18name]" +
                                                  ",[remark18value]" +
                                                  ",[remark19name]" +
                                                  ",[remark19value]" +
                                                  ",[remark20name]" +
                                                  ",[remark20value]" +
                                                  ",[externalSave]" +
                                                  ",[linha])" +
                                            " SELECT " +
                                                  " '" + SyabaseDR.GetValue(0).ToString().Replace("'", "''") + "' " +
                                                  ", '" + SyabaseDR.GetValue(1).ToString().Replace("'", "''") + "'" +
                                                  ", CONVERT(DATETIME, '" + SyabaseDR.GetValue(2).ToString() + "', " + dateConvert + ")" +
                                                  ", CONVERT(DATETIME, '" + SyabaseDR.GetValue(3).ToString() + "', " + dateConvert + ")" +
                                                  ", '" + SyabaseDR.GetValue(4).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(5).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(6).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(7).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(8).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(9).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(10).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(11).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(12).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(13).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(14).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(15).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(16).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(17).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(18).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(19).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(20).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(21).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(22).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(23).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(24).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(25).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(26).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(27).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(28).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(29).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(30).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(31).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(32).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(33).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(34).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(35).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(36).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(37).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(38).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(39).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(40).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(41).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(42).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(43).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(44).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(45).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(46).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(47).ToString().Replace("'", "''") + "'" +
                                                  ", '" + SyabaseDR.GetValue(48).ToString().Replace("'", "''") + "'" +
                                                  "," + linha.ToString() + " WHERE NOT EXISTS " +
                                                  " (SELECT [requestNo] FROM [request] WHERE [requestNo] = '" + SyabaseDR.GetValue(0).ToString().Replace("'", "''") + "' AND [linha] = '" + linha.ToString() + "' ) ";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados)
                        {
                            if (primeiraLinha)
                            {
                                primeiraLinha = false;
                            }
                            else
                            {
                                contador += SQLcmd.ExecuteNonQuery();
                            }
                        }

                    }
                    Console.WriteLine("gravar tabela Request (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela Request: " + e.Message);
                }

                try
                {
                    #region GRAVA TOTALDEFECT
                    SybaseCmd.CommandText = "SELECT top(100) requestNo, deviceNo, systemNo, defectType, totalPieceCountS0, totalPieceCountS1, totalPieceCountS2 FROM \"result\".\"totalDefect\" ORDER BY requestNo DESC ";

                    SyabaseDR = SybaseCmd.ExecuteReader();

                    primeiraBarra = true;
                    while (SyabaseDR.Read())
                    {
                        sql = "INSERT INTO[dbo].[totalDefect]" +
                              "([requestNo]" +
                              ",[deviceNo]" +
                              ",[systemNo]" +
                              ",[defectType]" +
                              ",[totalPieceCountS0]" +
                              ",[totalPieceCountS1]" +
                              ",[totalPieceCountS2]" +
                              ",[linha])" +
                        " SELECT " +
                              " '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(6).ToString() + "'" +
                              ", " + linha.ToString() + " WHERE NOT EXISTS  " +
                              " (SELECT [requestNo] from [totalDefect] WHERE [requestNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [deviceNo] = '"+ SyabaseDR.GetValue(1).ToString() + "'  AND [defectType] = '" + SyabaseDR.GetValue(3).ToString() + "' AND [linha] = '" + linha.ToString() + "') ";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados)
                        {
                            if (primeiraLinha)
                            {
                                primeiraLinha = false;
                            }
                            else
                            {
                                contador += SQLcmd.ExecuteNonQuery();
                            }
                        }

                    }

                    Console.WriteLine("gravar tabela TotalDefect (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    SyabaseDR.myDispose();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela TotalDefect: " + e.Message);
                }

                try
                {
                    #region GRAVA TOTALPIECE
                    SybaseCmd.CommandText = "SELECT top(100) requestNo, totalPieceSumNo, totalPieceCount, totalPieceCountS0, totalPieceCountS1, totalPieceCountS2, totalPieceLength, totalPieceLengthS0, totalPieceLengthS1, totalPieceLengthS2 FROM \"result\".\"totalPiece\" ORDER BY requestNo DESC";

                    SyabaseDR = SybaseCmd.ExecuteReader();

                    primeiraBarra = true;
                    while (SyabaseDR.Read())
                    {
                        sql = "INSERT INTO[dbo].[totalPiece]" +
                              "([requestNo]" +
                              ",[totalPieceSumNo]" +
                              ",[totalPieceCount]" +
                              ",[totalPieceCountS0]" +
                              ",[totalPieceCountS1]" +
                              ",[totalPieceCountS2]" +
                              ",[totalPieceLength]" +
                              ",[totalPieceLengthS0]" +
                              ",[totalPieceLengthS1]" +
                              ",[totalPieceLengthS2]" +
                              ",[linha])" +
                        " SELECT  " +
                              " '" + SyabaseDR.GetValue(0).ToString() + "' " +
                              ", '" + SyabaseDR.GetValue(1).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(2).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(3).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(4).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(5).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(6).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(7).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(8).ToString() + "'" +
                              ", '" + SyabaseDR.GetValue(9).ToString() + "'" +
                              ", " + linha.ToString() + " WHERE NOT EXISTS  " +
                              " (SELECT [requestNo] from [totalPiece] WHERE [requestNo] = '" + SyabaseDR.GetValue(0).ToString() + "' AND [totalPieceSumNo] = '"+ SyabaseDR.GetValue(1).ToString() + "'  AND [linha] = '" + linha.ToString() + "') ";

                        SQLcmd = new SqlCommand(sql, SQLcnn);
                        if (InserirDados)
                        {
                            if (primeiraLinha)
                            {
                                primeiraLinha = false;
                            }
                            else
                            {
                                contador += SQLcmd.ExecuteNonQuery();
                            }
                        }

                    }

                    Console.WriteLine("gravar tabela TotalPiece (LINHAS): " + contador.ToString());

                    SyabaseDR.Close();
                    SyabaseDR.myDispose();
                    System.Threading.Thread.Sleep(400);
                    #endregion
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela TotalPiece: " + e.Message);
                }


                try
                {
                    for (int i = 0; i < OPsList.Count; i++)
                    {
                        //Console.WriteLine("OPlist[" + i.ToString() + "]" + OPsList[i].toText());

                        if (OPsList[i].verificaIntegridade())
                        {

                            #region CAPTURA DEFEITOS PARA DADOS GERAL
                            SybaseCmd.CommandText = "SELECT defectNo, pieceNo, deviceNo, defectPosition, defectExtension, defectClass, defectAmplitude, eventLineNo, defectValuation, defectAddInfo1, defectAddInfo2, defectAddInfo3, defectCode FROM \"result\".\"defect\" WHERE pieceNo BETWEEN '" + OPsList[i].pieceNoStart + "' AND '" + OPsList[i].pieceNoEnd + "' ";
                            SyabaseDR = SybaseCmd.ExecuteReader();

                            DadosDefeitosGeral ret = new DadosDefeitosGeral();

                            while (SyabaseDR.Read())
                            {
                                int deviceNo;
                                if (int.TryParse(SyabaseDR.GetValue(2).ToString(), out deviceNo))
                                {
                                    switch (deviceNo)
                                    {
                                        case 1:
                                            ret.totalCF += 1;
                                            break;
                                        case 2:
                                            ret.totalUS += 1;
                                            break;
                                    }
                                }

                                #region RangeDefeitos 0-200+
                                int amplitude, extensao;

                                if (int.TryParse(SyabaseDR.GetValue(6).ToString(), out amplitude))
                                {
                                    int.TryParse(SyabaseDR.GetValue(4).ToString(), out extensao);
                                    ret.todosvaloresExtensao.Add(extensao);


                                    if (amplitude >= 0 && amplitude <= 9)
                                    {
                                        ret.AmplitudeRange0.extensaoTotal += extensao;
                                        ret.AmplitudeRange0.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 10 && amplitude <= 19)
                                    {
                                        ret.AmplitudeRange1.extensaoTotal += extensao;
                                        ret.AmplitudeRange1.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 20 && amplitude <= 29)
                                    {
                                        ret.AmplitudeRange2.extensaoTotal += extensao;
                                        ret.AmplitudeRange2.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 30 && amplitude <= 39)
                                    {
                                        ret.AmplitudeRange3.extensaoTotal += extensao;
                                        ret.AmplitudeRange3.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 40 && amplitude <= 49)
                                    {
                                        ret.AmplitudeRange4.extensaoTotal += extensao;
                                        ret.AmplitudeRange4.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 50 && amplitude <= 59)
                                    {
                                        ret.AmplitudeRange5.extensaoTotal += extensao;
                                        ret.AmplitudeRange5.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 60 && amplitude <= 69)
                                    {
                                        ret.AmplitudeRange6.extensaoTotal += extensao;
                                        ret.AmplitudeRange6.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 70 && amplitude <= 79)
                                    {
                                        ret.AmplitudeRange7.extensaoTotal += extensao;
                                        ret.AmplitudeRange7.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 80 && amplitude <= 89)
                                    {
                                        ret.AmplitudeRange8.extensaoTotal += extensao;
                                        ret.AmplitudeRange8.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 90 && amplitude <= 99)
                                    {
                                        ret.AmplitudeRange9.extensaoTotal += extensao;
                                        ret.AmplitudeRange9.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 100 && amplitude <= 109)
                                    {
                                        ret.AmplitudeRange10.extensaoTotal += extensao;
                                        ret.AmplitudeRange10.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 110 && amplitude <= 119)
                                    {
                                        ret.AmplitudeRange11.extensaoTotal += extensao;
                                        ret.AmplitudeRange11.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 120 && amplitude <= 129)
                                    {
                                        ret.AmplitudeRange12.extensaoTotal += extensao;
                                        ret.AmplitudeRange12.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 130 && amplitude <= 139)
                                    {
                                        ret.AmplitudeRange13.extensaoTotal += extensao;
                                        ret.AmplitudeRange13.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 140 && amplitude <= 149)
                                    {
                                        ret.AmplitudeRange14.extensaoTotal += extensao;
                                        ret.AmplitudeRange14.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 150 && amplitude <= 159)
                                    {
                                        ret.AmplitudeRange15.extensaoTotal += extensao;
                                        ret.AmplitudeRange15.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 160 && amplitude <= 169)
                                    {
                                        ret.AmplitudeRange16.extensaoTotal += extensao;
                                        ret.AmplitudeRange16.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 170 && amplitude <= 179)
                                    {
                                        ret.AmplitudeRange17.extensaoTotal += extensao;
                                        ret.AmplitudeRange17.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 180 && amplitude <= 189)
                                    {
                                        ret.AmplitudeRange18.extensaoTotal += extensao;
                                        ret.AmplitudeRange18.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 190 && amplitude <= 199)
                                    {
                                        ret.AmplitudeRange19.extensaoTotal += extensao;
                                        ret.AmplitudeRange19.valoresExtensao.Add(extensao);
                                    }
                                    if (amplitude >= 200)
                                    {
                                        ret.AmplitudeRange20.extensaoTotal += extensao;
                                        ret.AmplitudeRange20.valoresExtensao.Add(extensao);
                                    }
                                }
                                #endregion
                            }

                            SyabaseDR.Close();
                            SyabaseDR.myDispose();
                            System.Threading.Thread.Sleep(400);
                            #endregion

                            #region GRAVA DADOS GERAL
                            SybaseCmd.CommandText = "SELECT requestNo, totalPieceSumNo, totalPieceCount, totalPieceCountS0, totalPieceCountS1, totalPieceCountS2, totalPieceLength, totalPieceLengthS0, totalPieceLengthS1, totalPieceLengthS2 FROM \"result\".\"totalPiece\" WHERE requestNo=" + OPsList[i].requestNo;

                            SyabaseDR = SybaseCmd.ExecuteReader();
                            string totalBarras, barrasRejeitadasGeral, barrasRejeitadasInterno, barrasRejeitadasSuperficial, barrasRejeitadasComprimento;
                            totalBarras = "";
                            barrasRejeitadasGeral = "";
                            barrasRejeitadasInterno = "";
                            barrasRejeitadasSuperficial = "";
                            barrasRejeitadasComprimento = "";

                            while (SyabaseDR.Read())
                            {
                                if (linha == 3 || linha == 1)
                                {
                                    switch ((int)SyabaseDR.GetValue(1)) //totalPieceSumNo
                                    {
                                        case -1:
                                            totalBarras = SyabaseDR.GetValue(2).ToString(); //totalPieceCount
                                            barrasRejeitadasGeral = SyabaseDR.GetValue(4).ToString(); //totalPieceCountS1
                                            break;
                                        case 1:
                                            barrasRejeitadasSuperficial = SyabaseDR.GetValue(4).ToString(); //totalPieceCountS1
                                            break;
                                        case 2:
                                            barrasRejeitadasInterno = SyabaseDR.GetValue(4).ToString(); //totalPieceCountS1
                                            break;
                                        case 5:
                                            barrasRejeitadasComprimento = SyabaseDR.GetValue(4).ToString(); //totalPieceCountS1
                                            break;
                                    }
                                }

                                if (linha == 2)
                                {
                                    switch ((int)SyabaseDR.GetValue(1)) //totalPieceSumNo
                                    {
                                        case -1:
                                            totalBarras = SyabaseDR.GetValue(2).ToString(); //totalPieceCount
                                            barrasRejeitadasGeral = (int.Parse(totalBarras) - (int)SyabaseDR.GetValue(3)).ToString(); // totalPieceCountS0
                                            barrasRejeitadasComprimento = SyabaseDR.GetValue(4).ToString(); //totalPieceCountS1
                                            break;
                                        case 1:
                                            barrasRejeitadasSuperficial = SyabaseDR.GetValue(5).ToString(); //totalPieceCountS2
                                            break;
                                        case 2:
                                            barrasRejeitadasInterno = SyabaseDR.GetValue(5).ToString(); //totalPieceCountS2
                                            break;
                                    }
                                }

                            }

                            if (OPsList[i].requestStart.Contains("M"))
                            {
                                dateConvert = "101";
                            }
                            else
                            {
                                dateConvert = "103";
                            }

                            int extensaoMedia = 0, extensaoMin = 0, extensaoMax = 0, qtdExt = 0;
                            double extensaoMediana = 0, extensaoDesvPad = 0;

                            qtdExt = ret.todosvaloresExtensao.Count;

                            if (qtdExt > 0)
                            {
                                ret.todosvaloresExtensao.Sort();
                                extensaoMin = ret.todosvaloresExtensao[0];
                                extensaoMax = ret.todosvaloresExtensao[qtdExt - 1];

                                extensaoMedia = ret.somaTodasExtensoes() / qtdExt;
                                extensaoDesvPad = ret.desvioPadraoTodos();


                                /*string aa = "valoresExtensao: ";
                                for (int ii = 0; ii < ret.todosvaloresExtensao.Count; ii++)
                                {

                                    aa += ret.todosvaloresExtensao[ii].ToString() + ",";

                                }
                                Console.WriteLine(aa);*/

                                int mid = qtdExt / 2;
                                extensaoMediana = (qtdExt % 2 != 0) ? (double)ret.todosvaloresExtensao[mid] : ((double)ret.todosvaloresExtensao[mid - 1] + (double)ret.todosvaloresExtensao[mid]) / 2.0d;                                
                            }

                            sql = "INSERT INTO [dbo].[dadosGeral] " +
                                  "([linha] " +
                                  ",[requestName] " +
                                  ",[requestStart] " +
                                  ",[totalBarras] " +
                                  ",[barrasRejeitadasGeral] " +
                                  ",[barrasRejeitadasInterno] " +
                                  ",[barrasRejeitadasSuperficial] " +
                                  //",[barrasRejeitadasComprimento] " +
                                  ",[totalDefeitosUS] " +
                                  ",[totalDefeitosCF] " +
                                  ",[ExtensaoMedia] " +
                                  ",[ExtensaoMin] " +
                                  ",[ExtensaoMax] " +
                                  ",[ExtensaoDesvPadrao] " +
                                  ",[ExtensaoMediana] " +
                                  ",[A00Frequencia] " +
                                  ",[A00Media] " +
                                  ",[A00DesvPadrao] " +
                                  ",[A01Frequencia] " +
                                  ",[A01Media] " +
                                  ",[A01DesvPadrao] " +
                                  ",[A02Frequencia] " +
                                  ",[A02Media] " +
                                  ",[A02DesvPadrao] " +
                                  ",[A03Frequencia] " +
                                  ",[A03Media] " +
                                  ",[A03DesvPadrao] " +
                                  ",[A04Frequencia] " +
                                  ",[A04Media] " +
                                  ",[A04DesvPadrao] " +
                                  ",[A05Frequencia] " +
                                  ",[A05Media] " +
                                  ",[A05DesvPadrao] " +
                                  ",[A06Frequencia] " +
                                  ",[A06Media] " +
                                  ",[A06DesvPadrao] " +
                                  ",[A07Frequencia] " +
                                  ",[A07Media] " +
                                  ",[A07DesvPadrao] " +
                                  ",[A08Frequencia] " +
                                  ",[A08Media] " +
                                  ",[A08DesvPadrao] " +
                                  ",[A09Frequencia] " +
                                  ",[A09Media] " +
                                  ",[A09DesvPadrao] " +
                                  ",[A10Frequencia] " +
                                  ",[A10Media] " +
                                  ",[A10DesvPadrao] " +
                                  ",[A11Frequencia] " +
                                  ",[A11Media] " +
                                  ",[A11DesvPadrao] " +
                                  ",[A12Frequencia] " +
                                  ",[A12Media] " +
                                  ",[A12DesvPadrao] " +
                                  ",[A13Frequencia] " +
                                  ",[A13Media] " +
                                  ",[A13DesvPadrao] " +
                                  ",[A14Frequencia] " +
                                  ",[A14Media] " +
                                  ",[A14DesvPadrao] " +
                                  ",[A15Frequencia] " +
                                  ",[A15Media] " +
                                  ",[A15DesvPadrao] " +
                                  ",[A16Frequencia] " +
                                  ",[A16Media] " +
                                  ",[A16DesvPadrao] " +
                                  ",[A17Frequencia] " +
                                  ",[A17Media] " +
                                  ",[A17DesvPadrao] " +
                                  ",[A18Frequencia] " +
                                  ",[A18Media] " +
                                  ",[A18DesvPadrao] " +
                                  ",[A19Frequencia] " +
                                  ",[A19Media] " +
                                  ",[A19DesvPadrao] " +
                                  ",[A20Frequencia] " +
                                  ",[A20Media] " +
                                  ",[A20DesvPadrao]) " +
                            " SELECT " +
                                  " " + linha.ToString() + " " +
                                  ", '" + OPsList[i].OP + "'" +
                                  ", CONVERT(DATETIME, '" + OPsList[i].requestStart.ToString() + "', " + dateConvert.ToString() + ")" +
                                  ", '" + totalBarras.ToString() + "' " +
                                  ", '" + barrasRejeitadasGeral.ToString() + "'" +
                                  ", '" + barrasRejeitadasInterno.ToString() + "'" +
                                  ", '" + barrasRejeitadasSuperficial.ToString() + "' " +
                                  //", '" + barrasRejeitadasComprimento.ToString() + "' " +
                                  ", '" + ret.totalUS.ToString() + "' " +
                                  ", '" + ret.totalCF.ToString() + "' " +
                                  ", '" + extensaoMedia.ToString() + "' " +
                                  ",'" + extensaoMin.ToString() + "' " +
                                  ",'" + extensaoMax.ToString() + "' " +
                                  ",'" + extensaoDesvPad.ToString().Replace(",", ".") + "' " +
                                  ",'" + extensaoMediana.ToString().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange0.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange0.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange0.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange1.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange1.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange1.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange2.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange2.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange2.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange3.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange3.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange3.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange4.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange4.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange4.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange5.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange5.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange5.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange6.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange6.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange6.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange7.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange7.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange7.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange8.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange8.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange8.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange9.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange9.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange9.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange10.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange10.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange10.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange11.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange11.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange11.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange12.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange12.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange12.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange13.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange13.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange13.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange14.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange14.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange14.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange15.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange15.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange15.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange16.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange16.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange16.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange17.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange17.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange17.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange18.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange18.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange18.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange19.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange19.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange19.desvioPadrao().ToString().Replace(",", ".") + "' " +
                                  ", '" + ret.AmplitudeRange20.valoresExtensao.Count.ToString() + "' " +
                                  ", '" + ret.AmplitudeRange20.mediaExtensao().ToString() + "' " +
                                  ", '" + ret.AmplitudeRange20.desvioPadrao().ToString().Replace(",", ".") + "' WHERE NOT EXISTS " +
                                  " (SELECT [requestName] from [dadosGeral] WHERE [requestName] = '" + OPsList[i].OP + "' AND [linha] = '" + linha.ToString() + "') ";

                            //Console.WriteLine("contador: " + contador.ToString());
                            //Console.WriteLine("sql: " + sql);

                            SQLcmd = new SqlCommand(sql, SQLcnn);
                            if (InserirDados) { contador += SQLcmd.ExecuteNonQuery(); }

                            SyabaseDR.Close();
                            SyabaseDR.myDispose();
                            System.Threading.Thread.Sleep(400);
                            #endregion

                        }
                    }

                    Console.WriteLine("gravar tabela dados geral (LINHAS): " + contador.ToString());
                }
                catch (Exception e)
                {
                    temErro = true;
                    log.Error("Erro ao gravar tabela dados geral: " + e.Message + "\n" + e.StackTrace);
                }


                SybaseCmd.Dispose();
                SQLcmd.Dispose();

                log.Info("Dados Inseridos com sucesso. Linha " + linha.ToString() + " - Linhas adicionadas: " + contador.ToString());

            }
            catch (Exception e)
            {
                temErro = true;
                log.Error("Erro grave ao gravar dados: " + e.Message);
            }

        }


    }
}
