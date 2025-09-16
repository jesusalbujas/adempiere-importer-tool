/******************************************************************************
 * Product: ADempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 2006-2017 ADempiere Foundation, All Rights Reserved.         *
 * This program is free software, you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * or (at your option) any later version.                                     *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY, without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program, if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 * For the text or an alternative of this public license, you may reach us    *
 * or via info@adempiere.net                                                  *
 * or https://github.com/adempiere/adempiere/blob/develop/license.html        *
 *****************************************************************************/
/** Generated Model - DO NOT CHANGE */
package org.ait.model;

import java.sql.ResultSet;
import java.util.Properties;
import org.adempiere.core.domains.models.*;
import org.compiere.model.I_Persistent;
import org.compiere.model.MTable;
import org.compiere.model.PO;
import org.compiere.model.POInfo;

/** Generated Model for AIT_ImportTemplate
 *  @author Adempiere (generated) 
 *  @version Release 3.9.4 - $Id$ */
public class X_AIT_ImportTemplate extends PO implements I_AIT_ImportTemplate, I_Persistent 
{

	/**
	 *
	 */
	private static final long serialVersionUID = 20250915L;

    /** Standard Constructor */
    public X_AIT_ImportTemplate (Properties ctx, int AIT_ImportTemplate_ID, String trxName)
    {
      super (ctx, AIT_ImportTemplate_ID, trxName);
      /** if (AIT_ImportTemplate_ID == 0)
        {
			setAIT_ImportTemplate_ID (0);
			setName (null);
        } */
    }

    /** Load Constructor */
    public X_AIT_ImportTemplate (Properties ctx, ResultSet rs, String trxName)
    {
      super (ctx, rs, trxName);
    }

    /** AccessLevel
      * @return 3 - Client - Org 
      */
    protected int get_AccessLevel()
    {
      return accessLevel.intValue();
    }

    /** Load Meta Data */
    protected POInfo initPO (Properties ctx)
    {
      POInfo poi = POInfo.getPOInfo (ctx, Table_ID, get_TrxName());
      return poi;
    }

    public String toString()
    {
      StringBuffer sb = new StringBuffer ("X_AIT_ImportTemplate[")
        .append(get_ID()).append("]");
      return sb.toString();
    }

	public org.adempiere.core.domains.models.I_AD_Tab getAD_Tab() throws RuntimeException
    {
		return (org.adempiere.core.domains.models.I_AD_Tab)MTable.get(getCtx(), org.adempiere.core.domains.models.I_AD_Tab.Table_Name)
			.getPO(getAD_Tab_ID(), get_TrxName());	}

	/** Set Tab.
		@param AD_Tab_ID 
		Tab within a Window
	  */
	public void setAD_Tab_ID (int AD_Tab_ID)
	{
		if (AD_Tab_ID < 1) 
			set_Value (COLUMNNAME_AD_Tab_ID, null);
		else 
			set_Value (COLUMNNAME_AD_Tab_ID, Integer.valueOf(AD_Tab_ID));
	}

	/** Get Tab.
		@return Tab within a Window
	  */
	public int getAD_Tab_ID () 
	{
		Integer ii = (Integer)get_Value(COLUMNNAME_AD_Tab_ID);
		if (ii == null)
			 return 0;
		return ii.intValue();
	}

	public org.adempiere.core.domains.models.I_AD_Window getAD_Window() throws RuntimeException
    {
		return (org.adempiere.core.domains.models.I_AD_Window)MTable.get(getCtx(), org.adempiere.core.domains.models.I_AD_Window.Table_Name)
			.getPO(getAD_Window_ID(), get_TrxName());	}

	/** Set Window.
		@param AD_Window_ID 
		Data entry or display window
	  */
	public void setAD_Window_ID (int AD_Window_ID)
	{
		if (AD_Window_ID < 1) 
			set_Value (COLUMNNAME_AD_Window_ID, null);
		else 
			set_Value (COLUMNNAME_AD_Window_ID, Integer.valueOf(AD_Window_ID));
	}

	/** Get Window.
		@return Data entry or display window
	  */
	public int getAD_Window_ID () 
	{
		Integer ii = (Integer)get_Value(COLUMNNAME_AD_Window_ID);
		if (ii == null)
			 return 0;
		return ii.intValue();
	}

	/** Set Header CSV.
		@param AIT_HeaderCSV Header CSV	  */
	public void setAIT_HeaderCSV (String AIT_HeaderCSV)
	{
		set_Value (COLUMNNAME_AIT_HeaderCSV, AIT_HeaderCSV);
	}

	/** Get Header CSV.
		@return Header CSV	  */
	public String getAIT_HeaderCSV () 
	{
		return (String)get_Value(COLUMNNAME_AIT_HeaderCSV);
	}

	/** Set Import Template ID.
		@param AIT_ImportTemplate_ID Import Template ID	  */
	public void setAIT_ImportTemplate_ID (int AIT_ImportTemplate_ID)
	{
		if (AIT_ImportTemplate_ID < 1) 
			set_ValueNoCheck (COLUMNNAME_AIT_ImportTemplate_ID, null);
		else 
			set_ValueNoCheck (COLUMNNAME_AIT_ImportTemplate_ID, Integer.valueOf(AIT_ImportTemplate_ID));
	}

	/** Get Import Template ID.
		@return Import Template ID	  */
	public int getAIT_ImportTemplate_ID () 
	{
		Integer ii = (Integer)get_Value(COLUMNNAME_AIT_ImportTemplate_ID);
		if (ii == null)
			 return 0;
		return ii.intValue();
	}

	/** Set Description.
		@param Description 
		Optional short description of the record
	  */
	public void setDescription (String Description)
	{
		set_Value (COLUMNNAME_Description, Description);
	}

	/** Get Description.
		@return Optional short description of the record
	  */
	public String getDescription () 
	{
		return (String)get_Value(COLUMNNAME_Description);
	}

	/** Set Name.
		@param Name 
		Alphanumeric identifier of the entity
	  */
	public void setName (String Name)
	{
		set_Value (COLUMNNAME_Name, Name);
	}

	/** Get Name.
		@return Alphanumeric identifier of the entity
	  */
	public String getName () 
	{
		return (String)get_Value(COLUMNNAME_Name);
	}

	/** Set Immutable Universally Unique Identifier.
		@param UUID 
		Immutable Universally Unique Identifier
	  */
	public void setUUID (String UUID)
	{
		set_Value (COLUMNNAME_UUID, UUID);
	}

	/** Get Immutable Universally Unique Identifier.
		@return Immutable Universally Unique Identifier
	  */
	public String getUUID () 
	{
		return (String)get_Value(COLUMNNAME_UUID);
	}
}