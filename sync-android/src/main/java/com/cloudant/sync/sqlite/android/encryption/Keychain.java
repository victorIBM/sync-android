/*
 * IBM Confidential OCO Source Materials
 * 
 * 5725-I43 Copyright IBM Corp. 2006, 2013
 * 
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 * 
*/

package com.cloudant.sync.sqlite.android.encryption;

import android.content.Context;
import android.content.SharedPreferences;

import com.cloudant.sync.sqlite.android.encryption.common.DatabaseConstants;

import org.json.JSONException;



public class Keychain {

     private static final String PREF_NAME_DPK = "dpk"; //$NON-NLS-1$
     private static final String PREFS_NAME_DPK = "dpkPrefs"; //$NON-NLS-1$
     private SharedPreferences prefs;
     
     protected Keychain (Context context) {
          this.prefs = context.getSharedPreferences (Keychain.PREFS_NAME_DPK,
               Context.MODE_PRIVATE);
     }

     /*protected Keychain (Context context, String identifier) {
          this.prefs = context.getSharedPreferences (Keychain.PREFS_NAME_DPK,
                  Context.MODE_PRIVATE);
     }*/
     
     public DPKBean getDPKBean (String username) throws JSONException {
          String dpkJSON = this.prefs.getString (buildTag(username), null);
          
          if (dpkJSON == null) {
               return null;
          }
          
          return new DPKBean (dpkJSON);
     }
     
     public boolean isDPKAvailable (String username) {
          return (this.prefs.getString (buildTag(username), null) != null);
     }
     
     public void setDPKBean (String username, DPKBean dpkBean) {
          SharedPreferences.Editor editor = this.prefs.edit();
          
          editor.putString (buildTag(username), dpkBean.toString());
          
          editor.commit();
     }
     
     public void destroy () {
          
          SharedPreferences.Editor editor = this.prefs.edit();
          
          editor.clear();
                    
          editor.commit();
     }
     
     //Builds tags like: dpk-[username]
     //examples: dpk-carlos, dpk-tim, dpk (default user)
     private String buildTag (String tag) {
          if (tag.equals (DatabaseConstants.DEFAULT_USERNAME)) {
               //Use pre-2.0 jsonstore dpk keychain key
               return Keychain.PREF_NAME_DPK;
          }
          return Keychain.PREF_NAME_DPK + "-" + tag; //$NON-NLS-1$
     }
}
