package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics;

import java.sql.*;
import org.w3c.dom.*;
import java.math.BigInteger;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p101admin.commonsv11.resource.logics.userpm.*;

import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.allbetsl30.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.livebetsl50.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.nonlivebetsl50.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.pendingbets.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.settledbets.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.daysperformance.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.monthperformance.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.betlist.leagueperformance.*;

public class BetListBL extends MSELogic
{   
	BigInteger kAllBetsL30TaskIds_Start = null;
	BigInteger kAllBetsL30TaskIds_End = null;

	BigInteger kLiveBetsL50TaskIds_Start = null;
	BigInteger kLiveBetsL50TaskIds_End = null;

	BigInteger kNonLiveBetsL50TaskIds_Start = null;
	BigInteger kNonLiveBetsL50TaskIds_End = null;

	BigInteger kPendingBetsTaskIds_Start = null;
	BigInteger kPendingBetsTaskIds_End = null;

	BigInteger kSettledBetsTaskIds_Start = null;
	BigInteger kSettledBetsTaskIds_End = null;

	BigInteger kDaysPerfTaskIds_Start = null;
	BigInteger kDaysPerfTaskIds_End = null;

	BigInteger kMonthPerfTaskIds_Start = null;
	BigInteger kMonthPerfTaskIds_End = null;

	BigInteger kLeaguePerfTaskIds_Start = null;
	BigInteger kLeaguePerfTaskIds_End = null;

   	public BetListBL ()
	{
		super ();		
		
	    kAllBetsL30TaskIds_Start = new BigInteger ("10651040871");
	    kAllBetsL30TaskIds_End = new BigInteger ("10651040873");	

		kLiveBetsL50TaskIds_Start = new BigInteger ("10651040874");
	    kLiveBetsL50TaskIds_End = new BigInteger ("10651040876");	

	    kNonLiveBetsL50TaskIds_Start = new BigInteger ("10651040877");
	    kNonLiveBetsL50TaskIds_End = new BigInteger ("10651040879");	

	    kPendingBetsTaskIds_Start = new BigInteger ("10651040880");
	    kPendingBetsTaskIds_End = new BigInteger ("10651040882");	

	    kSettledBetsTaskIds_Start = new BigInteger ("10651040883");
	    kSettledBetsTaskIds_End = new BigInteger ("10651040885");

	    kDaysPerfTaskIds_Start = new BigInteger ("10651040886");
	    kDaysPerfTaskIds_End = new BigInteger ("10651040888");

	    kMonthPerfTaskIds_Start = new BigInteger ("10651040889");
	    kMonthPerfTaskIds_End = new BigInteger ("10651040892");

	    kLeaguePerfTaskIds_Start = new BigInteger ("10651040893");
	    kLeaguePerfTaskIds_End = new BigInteger ("10651040896");
	}
	
	/**
        A template method which has been extended from MSELogic.

        @see enlj.component.resource.logics.MSELogic#executeTask(Document oDocument, String oTaskId).
    */   
	public String executeTask (Document oDocument, String oTaskId)
	{		
	    BigInteger biTaskId = new BigInteger (oTaskId);
	    
		String oXMLString = "";
		setParams(oDocument);			
        
		if ((biTaskId.compareTo (kAllBetsL30TaskIds_Start) == 1 || biTaskId.compareTo (kAllBetsL30TaskIds_Start) == 0) && 
			(biTaskId.compareTo (kAllBetsL30TaskIds_End) == -1 || biTaskId.compareTo (kAllBetsL30TaskIds_End) == 0))
		{
			AllBetsL30BL oAllBetsL30BL = new AllBetsL30BL ();
			oXMLString = oAllBetsL30BL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kLiveBetsL50TaskIds_Start) == 1 || biTaskId.compareTo (kLiveBetsL50TaskIds_Start) == 0) && 
			(biTaskId.compareTo (kLiveBetsL50TaskIds_End) == -1 || biTaskId.compareTo (kLiveBetsL50TaskIds_End) == 0))
		{
			LiveBetsL50BL oLiveBetsL50BL = new LiveBetsL50BL ();
			oXMLString = oLiveBetsL50BL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kNonLiveBetsL50TaskIds_Start) == 1 || biTaskId.compareTo (kNonLiveBetsL50TaskIds_Start) == 0) && 
			(biTaskId.compareTo (kNonLiveBetsL50TaskIds_End) == -1 || biTaskId.compareTo (kNonLiveBetsL50TaskIds_End) == 0))
		{
			NonLiveBetsL50BL oNonLiveBetsL50BL = new NonLiveBetsL50BL ();
			oXMLString = oNonLiveBetsL50BL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kPendingBetsTaskIds_Start) == 1 || biTaskId.compareTo (kPendingBetsTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kPendingBetsTaskIds_End) == -1 || biTaskId.compareTo (kPendingBetsTaskIds_End) == 0))
		{
			PendingBetsBL oPendingBetsBL = new PendingBetsBL ();
			oXMLString = oPendingBetsBL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kSettledBetsTaskIds_Start) == 1 || biTaskId.compareTo (kSettledBetsTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kSettledBetsTaskIds_End) == -1 || biTaskId.compareTo (kSettledBetsTaskIds_End) == 0))
		{
			SettledBetsBL oSettledBetsBL = new SettledBetsBL ();
			oXMLString = oSettledBetsBL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kDaysPerfTaskIds_Start) == 1 || biTaskId.compareTo (kDaysPerfTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kDaysPerfTaskIds_End) == -1 || biTaskId.compareTo (kDaysPerfTaskIds_End) == 0))
		{
			DaysPerformanceBL oDaysPerformanceBL = new DaysPerformanceBL ();
			oXMLString = oDaysPerformanceBL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kMonthPerfTaskIds_Start) == 1 || biTaskId.compareTo (kMonthPerfTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kMonthPerfTaskIds_End) == -1 || biTaskId.compareTo (kMonthPerfTaskIds_End) == 0))
		{
			MonthPerformanceBL oMonthPerformanceBL = new MonthPerformanceBL ();
			oXMLString = oMonthPerformanceBL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kLeaguePerfTaskIds_Start) == 1 || biTaskId.compareTo (kLeaguePerfTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kLeaguePerfTaskIds_End) == -1 || biTaskId.compareTo (kLeaguePerfTaskIds_End) == 0))
		{
			LeaguePerformanceBL oLeaguePerformanceBL = new LeaguePerformanceBL ();
			oXMLString = oLeaguePerformanceBL.executeTask (oDocument, oTaskId);
		}

		return oXMLString;
	}		    	    
    
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}