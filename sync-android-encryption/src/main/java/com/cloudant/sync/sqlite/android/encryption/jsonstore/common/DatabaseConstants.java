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

package com.cloudant.sync.sqlite.android.encryption.jsonstore.common;

public class DatabaseConstants {
	public static final String FIELD_DELETED = "_deleted"; //$NON-NLS-1$
	public static final String FIELD_DIRTY = "_dirty"; //$NON-NLS-1$
	public static final String FIELD_ID = "_id"; //$NON-NLS-1$
	public static final String FIELD_JSON = "json"; //$NON-NLS-1$
	public static final String FIELD_OPERATION = "_operation"; //$NON-NLS-1$
	public static final String OPERATION_ADD = "add"; //$NON-NLS-1$
	public static final String OPERATION_REMOVE = "remove"; //$NON-NLS-1$
	public static final String OPERATION_REPLACE = "replace"; //$NON-NLS-1$
	public static final String OPERATION_STORE = "store"; //$NON-NLS-1$

	public static final String OLD_DB_PATH = "com.ibm.worklight.database"; //Used in V1 //$NON-NLS-1$
	public static final String DB_SUB_DIR = "wljsonstore"; //$NON-NLS-1$
	public static final String DB_PATH_EXT = ".sqlite"; //$NON-NLS-1$
	public static final String DEFAULT_USERNAME = "jsonstore"; //$NON-NLS-1$

	public static final String JSONSTORE_PREFS = "JsonstorePrefs"; //$NON-NLS-1$
	public static final String JSONSTORE_VERSION_PREF = "JsonstoreVer"; //$NON-NLS-1$
	
	public static final String ASCENDING = "ASC"; //$NON-NLS-1$
	public static final String DESCENDING = "DESC"; //$NON-NLS-1$

	public static final String SQL_AND = " AND "; //$NON-NLS-1$
	public static final String SQL_EQ = " = ?"; //$NON-NLS-1$
	public static final String SQL_NOT_EQ = " != ?"; //$NON-NLS-1$
	public static final String SQL_GT = " > ?"; //$NON-NLS-1$
	public static final String SQL_LT = " < ?"; //$NON-NLS-1$
	public static final String SQL_GTE = " >= ?"; //$NON-NLS-1$
	public static final String SQL_LTE = " <= ?"; //$NON-NLS-1$
	public static final String SQL_LIKE = " LIKE ?"; //$NON-NLS-1$
	public static final String SQL_NOT_BETWEEN = " NOT BETWEEN ? AND ? "; //$NON-NLS-1$
	public static final String SQL_BETWEEN = " BETWEEN ? AND ? "; //$NON-NLS-1$
	public static final String SQL_IN = " IN "; //$NON-NLS-1$
	public static final String SQL_NOT_IN = " NOT IN "; //$NON-NLS-1$
	public static final String SQL_NOT_LIKE = " NOT LIKE ?"; //$NON-NLS-1$
	public static final String SQL_OR = " OR "; //$NON-NLS-1$
	public static final String SQL_LIMIT = " LIMIT "; //$NON-NLS-1$
	public static final String SQL_OFFSET = " OFFSET "; //$NON-NLS-1$
	public static final String SQL_SORT = " ORDER BY "; //$NON-NLS-1$
	public static final String SQL_FROM = " FROM "; //$NON-NLS-1$
	public static final String SQL_UPDATE = " UPDATE "; //$NON-NLS-1$
	public static final String SQL_SET = " SET "; //$NON-NLS-1$
	public static final String SQL_WHERE = " WHERE "; //$NON-NLS-1$
	public static final String SQL_COUNT = "count(*)"; //$NON-NLS-1$
	public static final String SQL_ALL = "*"; //$NON-NLS-1$
	public static final String SQL_SELECT = " SELECT "; //$NON-NLS-1$
	
	
	public static final int RC_DESTROY_FAILED_FILE_ERROR = -18;
	public static final int RC_DESTROY_FAILED_METADATA_REMOVAL_FAILURE = -19;
	public static final int RC_COULD_NOT_MARK_DOCUMENT_PUSHED = 15;
	public static final int RC_DB_NOT_OPEN = -50;
	public static final int RC_TRANSACTION_FAILURE_DURING_ROLLBACK = -48;
	public static final int RC_TRANSACTION_FAILURE_DURING_REMOVE_COLLECTION = -47;
	public static final int RC_TRANSACTION_FAILURE_DURING_DESTROY = -46;
	public static final int RC_TRANSACTION_FAILURE_DURING_CLOSE_ALL = -45;
	public static final int RC_TRANSACTION_DURING_INIT = -44;
	public static final int RC_TRANSACTION_FAILURE = -43;
	public static final int RC_NO_TRANSACTION_IN_PROGRESS = -42;
	public static final int RC_TRANSACTION_IN_PROGRESS = -41;
	public static final int RC_FIPS_FAILURE = -40;
	public static final int RC_DESTROY_FILE_FAILED = -5;
	public static final int RC_DESTROY_KEYS_FAILED = -4;
	public static final int RC_ERROR_CHANGING_PASSWORD = 24;
	public static final int RC_ERROR_CLEARING_COLLECTION = 26;
	public static final int RC_ERROR_CLOSING_ALL = 23;
	public static final int RC_ERROR_DURING_DESTROY = 25;
	public static final int RC_INVALID_SEARCH_FIELD = 22;
	public static final int RC_TABLE_CREATED = 0;
	public static final int RC_TABLE_EXISTS = 1;
	public static final int RC_TABLE_FAILURE = -1;
	public static final int RC_TABLE_KEY_FAILURE = -3;
	public static final int RC_TABLE_SCHEMA_MISMATCH = -2;
	public static final int RC_USERNAME_MISMATCH_DETECTED = -6;
	public static final int RC_ERROR_INVALID_SORT_OBJECT = 28;
	public static final int RC_ERROR_INVALID_FILTER_ARRAY = 29;

	private DatabaseConstants() {
	}
}
