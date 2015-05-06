package com.cloudant.sync.sqlite.android;

import com.cloudant.sync.sqlite.Cursor;

/**
 * Created by estebanmlaver.
 */
public class AndroidSQLCipherSQLiteCursor implements Cursor {

    private net.sqlcipher.Cursor internalCursor;

    public AndroidSQLCipherSQLiteCursor(net.sqlcipher.Cursor cursor) {
        this.internalCursor = cursor;
    }

    @Override
    public int getCount() {
        return this.internalCursor.getCount();
    }

    @Override
    public int columnType(int index) {
        return this.internalCursor.getType(index);
    }

    @Override
    public String columnName(int index) {
        return this.internalCursor.getColumnName(index);
    }

    @Override
    public boolean moveToFirst() {
        return this.internalCursor.moveToFirst();
    }

    @Override
    public String getString(int index) {
        return this.internalCursor.getString(index);
    }

    @Override
    public int getInt(int index) {
        return this.internalCursor.getInt(index);
    }

    @Override
    public long getLong(int index) {
        return this.internalCursor.getLong(index);
    }

    @Override
    public float getFloat(int index) {
        return this.internalCursor.getFloat(index);
    }

    @Override
    public byte[] getBlob(int index) {
        return this.internalCursor.getBlob(index);
    }

    @Override
    public boolean isAfterLast() {
        return this.internalCursor.isAfterLast();
    }

    @Override
    public boolean moveToNext() {
        return this.internalCursor.moveToNext();
    }

    @Override
    public void close() {
        this.internalCursor.close();
    }

    @Override
    public int getColumnCount() {
        return this.internalCursor.getColumnCount();
    }

    @Override
    public int getColumnIndex(String columnName){
        return this.internalCursor.getColumnIndex(columnName);
    }

    @Override
    public int getColumnIndexOrThrow(String columnName) throws IllegalArgumentException {
        return this.internalCursor.getColumnIndexOrThrow(columnName);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.internalCursor.close();
    }
}
