package enlj.p106trading.mssqlv51.p10651sport.teampointsreduction.logics;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.dleaguetable.anormaltable.logics.*;

public class TeamPointsReductionBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510107";
    
	/* Task Ids */		
	private final String kInitData		= "10651010701";
	private final String kLeagues		= "10651010704";
	private final String kSeasons		= "10651010705";
	private final String kReductionList	= "10651010706";	
	private final String kUpdate	    = "10651010707";
	
	/* Server Row Names */
	private final String kSR_Countries		= "sr1";       		
	private final String kSR_Leagues		= "sr2";	
	private final String kSR_Seasons		= "sr3";	
	private final String kSR_ReductionList	= "sr4";    

	/* Client Row Names */	
	private final String kCR_CountryId		= "cr1"; 
	private final String kCR_LeagueId		= "cr2";
	private final String kCR_ReductionList	= "cr3";	
	private final String kCR_Update	        = "cr4";	
	private final String kCR_LeagueTable	= "cr5";

	/* Status Ids */
    private final String GT_ReductionList	= "51";	
	
    private final String T1_FunctionPM  = "101";
    private final String T1_Countries	= "102";    	
			
    private final String T4_Leagues	= "401";
    private final String T5_Seasons	= "501";
    
	private final String T7_Delete  = "701";  
    private final String T7_Insert	= "702";    
        
    /* Leagues Index */
    private final int f_CountryId = 0;
    
    /* Seasons Index */
    private final int f_LeagueId = 0;
    
    /* Reduction List Index */
    private final int f_LLeagueId = 0;
    private final int f_LSeasonId = 1;
    
    /* Update Team Points Reduction Fields */
    private final int f_ULeagueId		= 0;
    private final int f_USeasonId		= 1;
    private final int f_UTeamId	        = 2;
    private final int f_UDeductPoints   = 3;
    private final int f_UComments		= 4;        
    			
	public TeamPointsReductionBL ()
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
			
		if (oTaskId.equals (kInitData))
		{
			oXMLString = getInitData ();
		}
		else if (oTaskId.equals (kLeagues))
		{
			oXMLString = getLeagues ();
		}
		else if (oTaskId.equals (kSeasons))
		{
			oXMLString = getSeasons ();
		}
		else if (oTaskId.equals (kReductionList))
		{
			oXMLString = getReductionList ();
		}	
		else if (oTaskId.equals (kUpdate))
		{
			oXMLString = insertTeamPoints ();
		}		

		return oXMLString;
	}	

	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();       
         
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));
        oBuffer.append (SoccerUtil.getLeagueCountries_Soc (this, T1_Countries, kSR_Countries, ConstantsUtil.kFE_ChooseOne));        
        
        return oBuffer.toString ();
    }
        
    private String getLeagues ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {
			String [] arrInfo = getParams (kCR_CountryId);			
			
            String oSQL =
				" Select en_0651b01_leagueinfo_soc.leagueid, " +
					" en_0651b01_leagueinfo_soc.leaguename_" + getLanguage () + " As leaguename, " +
					" 1 As orderid " +
				" From en_0651b01_leagueinfo_soc " +
				" Where en_0651b01_leagueinfo_soc.leagueid > 0 And " +
					" en_0651b01_leagueinfo_soc.countryid = " + convertToInt (arrInfo [f_CountryId]) +
				" Union All " +
				" Select 0 As leagueid, " +
					" en_0651z00_firstelement.name_" + getLanguage () + " As leaguename, " +
					" 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_ChooseOne +
				" Order By orderid, leaguename ";			                			

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Leagues)); 
				oBuffer.append (getStatusXML (T4_Leagues, 1, "TeamPointsReductionBL:getLeagues:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_Leagues, -1, "TeamPointsReductionBL:getLeagues:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_Leagues, -1, "TeamPointsReductionBL:getLeagues:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }
    
    private String getSeasons ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {
			String [] arrInfo = getParams (kCR_LeagueId);			
			
            String oSQL =
				" Select en_0651b06_seasoninfo_soc.seasonid, " +
					" en_0651b06_seasoninfo_soc.seasonname_" + getLanguage () + " As seasonname, " +
					" 1 As orderid " +
				" From en_0651b06_seasoninfo_soc " +
				" Where en_0651b06_seasoninfo_soc.leagueid = " + convertToInt (arrInfo [f_LeagueId]) +
				" Union All " +
				" Select 0 As seasonid, " +
					" en_0651z00_firstelement.name_" + getLanguage () + " As seasonname, " +
					" 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_ChooseOne +
				" Order By orderid, seasonname Desc ";				                			

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Seasons)); 
				oBuffer.append (getStatusXML (T5_Seasons, 1, "TeamPointsReductionBL:getSeasons:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Seasons, -1, "TeamPointsReductionBL:getSeasons:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Seasons, -1, "TeamPointsReductionBL:getSeasons:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }
    
    private String getReductionList ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {
			String [] arrInfo = getParams (kCR_ReductionList);
			int nLeagueId = convertToInt (arrInfo [f_LLeagueId]);
			int nSeasonId = convertToInt (arrInfo [f_LSeasonId]);			
			
            String oSQL =
				" Select Distinct en_0651b03_teaminfo_soc.teamid, " +
					" en_0651b03_teaminfo_soc.teamname_en As teamname, " +
					" en_0651b07_teampointsreduction_soc.deductpoints, " +
					" en_0651b07_teampointsreduction_soc.comments, " +
					" en_0151a04_userinfo.username, " +
					" Convert (varchar, en_0651b07_teampointsreduction_soc.createddate, 103) As createddate, " +
					" Convert (varchar (5), en_0651b07_teampointsreduction_soc.createddate, 108) As createdtime " +
				" From en_0651b08_scheduleinfo_soc, en_0651b01_leagueinfo_soc, en_0651b06_seasoninfo_soc, " +
					" en_0651b03_teaminfo_soc, en_0651b07_teampointsreduction_soc, en_0151a04_userinfo " +
				" Where en_0651b08_scheduleinfo_soc.leagueid = en_0651b01_leagueinfo_soc.leagueid And " +
					" en_0651b01_leagueinfo_soc.leagueid = en_0651b06_seasoninfo_soc.leagueid And " +
					" (en_0651b08_scheduleinfo_soc.ateamid = en_0651b03_teaminfo_soc.teamid OR " +
						" en_0651b08_scheduleinfo_soc.bteamid = en_0651b03_teaminfo_soc.teamid) And " +
					" en_0651b03_teaminfo_soc.teamid = en_0651b07_teampointsreduction_soc.teamid And " +
					" en_0651b07_teampointsreduction_soc.leagueid = en_0651b06_seasoninfo_soc.leagueid And " +
					" en_0651b07_teampointsreduction_soc.seasonid = en_0651b06_seasoninfo_soc.seasonid And " +
					" en_0651b07_teampointsreduction_soc.createdby = en_0151a04_userinfo.userid And " +
					" en_0651b03_teaminfo_soc.teamid > 0 And " +
					" en_0651b01_leagueinfo_soc.leagueid = " + nLeagueId + " And " +
					" en_0651b06_seasoninfo_soc.seasonid = " + nSeasonId + " And " +
					" en_0651b08_scheduleinfo_soc.scheduledate Between " +
						" en_0651b06_seasoninfo_soc.startdate And en_0651b06_seasoninfo_soc.enddate " +
				" Union All " +
				" Select Distinct en_0651b03_teaminfo_soc.teamid, " +
					" en_0651b03_teaminfo_soc.teamname_en As teamname, " +
					" 0 As deductpoints, " +
					" '-' As comments, " +
					" '-' As username, " +
					" Convert (varchar, GetDate (), 103) As createddate, " +
					" Convert (varchar (5), GetDate (), 108) As createdtime " +
				" From en_0651b08_scheduleinfo_soc, en_0651b01_leagueinfo_soc, " +
					" en_0651b06_seasoninfo_soc, en_0651b03_teaminfo_soc " +
				" Where en_0651b08_scheduleinfo_soc.leagueid = en_0651b01_leagueinfo_soc.leagueid And " +
					" en_0651b01_leagueinfo_soc.leagueid = en_0651b06_seasoninfo_soc.leagueid And " +
					" (en_0651b08_scheduleinfo_soc.ateamid = en_0651b03_teaminfo_soc.teamid OR " +
						" en_0651b08_scheduleinfo_soc.bteamid = en_0651b03_teaminfo_soc.teamid) And " +
					" en_0651b03_teaminfo_soc.teamid Not In " +
					" ( " +
						" Select en_0651b07_teampointsreduction_soc.teamid " +
						" From en_0651b07_teampointsreduction_soc " +
						" Where en_0651b07_teampointsreduction_soc.leagueid = " + nLeagueId + " And " +
							" en_0651b07_teampointsreduction_soc.seasonid = " + nSeasonId +
					" ) And " +
					" en_0651b03_teaminfo_soc.teamid > 0 And " +
					" en_0651b01_leagueinfo_soc.leagueid = " + nLeagueId + " And " +
					" en_0651b06_seasoninfo_soc.seasonid = " + nSeasonId + " And " +
					" en_0651b08_scheduleinfo_soc.scheduledate Between " +
						" en_0651b06_seasoninfo_soc.startdate And en_0651b06_seasoninfo_soc.enddate " +
				" Order By teamname ";
				                
			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_ReductionList)); 
				oBuffer.append (getStatusXML (GT_ReductionList, 1, "TeamPointsReductionBL:getReductionList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (GT_ReductionList, -1, "TeamPointsReductionBL:getReductionList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (GT_ReductionList, -1, "TeamPointsReductionBL:getReductionList:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

		oBuffer.append (getLeagueTable ());
        return oBuffer.toString ();
    }                			        
	
	private String getLeagueTable ()
    {
		StringBuffer oBuffer = new StringBuffer ();
        
        int nSportId = SportUtil.kS_Soccer;
                    
		String [] arrInfo = getParams (kCR_LeagueTable);				
		LeagueTableObj oLogic = new LeagueTableObj (getDocument ());
		oBuffer.append (oLogic.getLeagueTable (nSportId, arrInfo));		

        return oBuffer.toString ();
	}

    private String insertTeamPoints ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		
        StringBuffer oBuffer = new StringBuffer ();		
        
        try
        {
            String [] arrInfo = getParams (kCR_Update);            
			int nLeagueId = convertToInt (arrInfo [f_ULeagueId]);
			int nSeasonId = convertToInt (arrInfo [f_USeasonId]);
			int nTeamId = convertToInt (arrInfo [f_UTeamId]);

            oBuffer.append (deleteTeamPoints (arrInfo));
            
			int nReducePoints = convertToInt (arrInfo [f_UDeductPoints]);
			if (nReducePoints != 0)
			{
				String oSQL =
				   " Insert Into en_0651b07_teampointsreduction_soc "+
					" ( " +
						" leagueid, seasonid, " +
						" teamid, deductpoints, " +
						" comments, " +
						" createdby, createddate, createdby_xuser " +					
					" ) " +
					" Values " +
					" ( " + 
						" " + arrInfo [f_ULeagueId] + ", " + arrInfo [f_USeasonId] + ", " + 
						" " + arrInfo [f_UTeamId] + ", " + arrInfo [f_UDeductPoints] + ", " + 
						" '" + arrInfo [f_UComments] + "', " + 					
						" " + getUserId () + ", " + TradingUtil.getDateString () + ", " + getUserId () + " " + 					
					" ) ";

				oStatement = oConnector.getStatement ();
			
				int nStatusId = oConnector.executeUpdate (oSQL, oStatement);
				if (nStatusId >= 0)
					oBuffer.append (getStatusXML (T7_Insert, 1, "TeamPointsReductionBL:insertTeamPoints:Successfull"));            
				else
					oBuffer.append (getStatusXML (T7_Insert, -1, "TeamPointsReductionBL:insertTeamPoints:UnSuccessfull"));
			}               			            
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T7_Insert, -1, "TeamPointsReductionBL:insertTeamPoints:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        oBuffer.append (getReductionList ());
        return oBuffer.toString ();
    }        

    private String deleteTeamPoints (String [] arrInfo)
	{
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {            
			int nLeagueId = convertToInt (arrInfo [f_ULeagueId]);
			int nSeasonId = convertToInt (arrInfo [f_USeasonId]);
			int nTeamId = convertToInt (arrInfo [f_UTeamId]);
			
			String oSQL = 
			    " Delete From en_0651b07_teampointsreduction_soc " +
			    " Where en_0651b07_teampointsreduction_soc.leagueid = " + nLeagueId + " And " +
					" en_0651b07_teampointsreduction_soc.seasonid = " + nSeasonId + " And " +
					" en_0651b07_teampointsreduction_soc.teamid = " + nTeamId;

			oStatement = oConnector.getStatement ();
				
			int nStatusId = oConnector.executeUpdate (oSQL, oStatement);
			if (nStatusId >= 0)
				oBuffer.append (getStatusXML (T7_Delete, 1, "TeamPointsReductionBL:deleteTeamPoints:Successfull"));				
			else
				oBuffer.append (getStatusXML (T7_Delete, -1, "TeamPointsReductionBL:deleteTeamPoints:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T7_Delete, -1, "TeamPointsReductionBL:deleteTeamPoints" + oException.toString ()));
            log (oBuffer.toString ());            
        }
        
        return oBuffer.toString ();
	}
    
	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}