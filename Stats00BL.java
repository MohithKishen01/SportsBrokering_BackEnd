package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics.statistics.stats00;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.estatistics.gteamstats.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.anormaltable.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.cnormalmatchwld.logics.*;

public class Stats00BL extends MSELogic
{
    /* Report Constants */
    private final int kRT_TeamStats             = 1;
    private final int kRT_LeagueTable           = 2;
    private final int kRT_Last12MatchesAll_HA	= 3;
    private final int kRT_Last12Matches_HA      = 4;
    private final int kRT_HeadToHeadTS_HA       = 5;

    /* Module Id */
    private final String kModuleId = "106510408";    	
    
	/* Task Ids */ 	 	
    private final String kA_MatchStats  = "10651040811";

	/* Server Row Names */
   
	/* Client Row Names */    
    private final String kCR_A_Report_Fltr          = "cr11";
    private final String kCR_A_LeagueTable          = "cr12";
    private final String kCR_A_TeamStats_Home       = "cr13";
    private final String kCR_A_TeamStats_Away       = "cr14";
    private final String kCR_A_Last12MatchesAll_HA  = "cr15";
    private final String kCR_A_Last12Matches_HA     = "cr16";
    private final String kCR_A_HeadToHeadTS_HA      = "cr17";
    
	/* Status Ids */		

    /* Match Stats Field */
    private final int f_MSReportTypeId = 0;
	
	/* Match Statistics */
   	public Stats00BL ()
	{
		super ();		
	}
	
	/**
        A template method which has been extended from MSELogic.

        @see enlj.component.resource.logics.MSELogic#executeTask(Document oDocument, String oTaskId).
    */   
	public String executeTask (Document oDocument, String oTaskId)
	{		
		String oXMLString = "";
		setParams(oDocument);			

        if (oTaskId.equals (kA_MatchStats))
        {
            oXMLString = getMatchStatsData ();
        }
       
		return oXMLString;
	}		
    
    private String getMatchStatsData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        String [] arrInfo = getParams (kCR_A_Report_Fltr);
        int nReportTypeId = convertToInt (arrInfo [f_MSReportTypeId]);
        
        switch (nReportTypeId)
        {
            case kRT_TeamStats :
                oBuffer.append (getTeamStats ());
                break;
            case kRT_LeagueTable :
                oBuffer.append (getLeagueTable ());
                break;
            case kRT_Last12MatchesAll_HA :
                oBuffer.append (getLast12MatchesAll_HA ());
                break;
            case kRT_Last12Matches_HA :
                oBuffer.append (getLast12Matches_HA ());
                break;
            case kRT_HeadToHeadTS_HA :
                oBuffer.append (getHeadToHeadTS_HA ());
                break;
        }
        
        return oBuffer.toString ();
    } 
    
    private String getTeamStats ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;

		oBuffer.append (getTeamStatsHome_A (nSportId));
		oBuffer.append (getTeamStatsAway_A (nSportId));

        return oBuffer.toString ();
	}
	
	private String getTeamStatsHome_A (int nSportId)
    {
        StringBuffer oBuffer = new StringBuffer ();
        
		String [] arrInfo = getParams (kCR_A_TeamStats_Home);						
	    TeamStatsObj oLogic = new TeamStatsObj (getDocument ());
	    oBuffer.append (oLogic.getTeamStats (nSportId, arrInfo));		
		
		return oBuffer.toString ();
    }

    private String getTeamStatsAway_A (int nSportId)
    {
		StringBuffer oBuffer = new StringBuffer ();
        
		String [] arrInfo = getParams (kCR_A_TeamStats_Away);						
	    TeamStatsObj oLogic = new TeamStatsObj (getDocument ());
	    oBuffer.append (oLogic.getTeamStats (nSportId, arrInfo));		
		
		return oBuffer.toString ();
    }
    
    private String getLeagueTable ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_BasketBall;

		String [] arrInfo = getParams (kCR_A_LeagueTable);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());
		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}
        
    private String getLast12MatchesAll_HA ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        MatchWLDObj oLogic = new MatchWLDObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_A_Last12MatchesAll_HA);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getMatchWLD (SportUtil.kS_BasketBall, arrInfo));
		}

        return oBuffer.toString ();
	}
	
	private String getLast12Matches_HA ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        MatchWLDObj oLogic = new MatchWLDObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_A_Last12Matches_HA);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getSideMatchWLD (SportUtil.kS_BasketBall, arrInfo));
		}

        return oBuffer.toString ();
	}
		
	private String getHeadToHeadTS_HA ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        TeamStatsObj oLogic = new TeamStatsObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_A_HeadToHeadTS_HA);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);
	        
		    oBuffer.append (oLogic.getTeamStats (SportUtil.kS_BasketBall, arrInfo));		
		}

        return oBuffer.toString ();
	}
	
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}