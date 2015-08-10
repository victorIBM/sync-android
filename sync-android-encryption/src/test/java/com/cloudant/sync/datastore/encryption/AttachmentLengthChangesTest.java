package com.cloudant.sync.datastore.encryption;

/**
 * Created by estebanmlaver on 8/6/15.
 */

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.util.Base64;

import com.cloudant.sync.datastore.Attachment;
import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.DatastoreManager;
import com.cloudant.sync.datastore.DocumentBodyFactory;
import com.cloudant.sync.datastore.DocumentRevision;
import com.cloudant.sync.datastore.DocumentRevisionBuilder;
import com.cloudant.sync.datastore.MutableDocumentRevision;
import com.cloudant.sync.datastore.UnsavedStreamAttachment;
import com.cloudant.sync.datastore.encryption.AndroidKeyProvider;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;

import static org.junit.Assume.assumeNotNull;

public class AttachmentLengthChangesTest {

    private String dbName;
    private Datastore datastore;
    private static KeyProvider keyProvider;
    private static DatastoreManager manager;

    private static final String BASE64_ENCODED_IMAGE = "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";
    private static final String CONTENT_TYPE = "image/jpeg";
    private static final String ATTACHMENT_NAME = "face";

    static {
        // Load SQLCipher libraries
        net.sqlcipher.database.SQLiteDatabase.loadLibs(ProviderTestUtil.getContext());
    }

    @Before
    public void beforeMethod(){
        assumeNotNull(ProviderTestUtil.getContext());
        if (keyProvider == null) {
            keyProvider = new CachingKeyProvider(new AndroidKeyProvider(
                    ProviderTestUtil.getContext(), "password", "identifier"));
        }

        if (manager == null) {
            // Setup DatastoreManager
            File path = ProviderTestUtil.getContext().getDir("datastores", Context.MODE_PRIVATE);
            manager = new DatastoreManager(path.getAbsolutePath());
        }

    }

    @Test
    public void testUnsavedStreamAttachment() throws Exception {
        // Create test db
        dbName = "test" + System.currentTimeMillis();
        datastore = manager.openDatastore(dbName);

        // Decode Base64 image and get bytes
        byte[] originalBytes = Base64.decode(BASE64_ENCODED_IMAGE, Base64.NO_WRAP);
        UnsavedStreamAttachment streamAttachment = new UnsavedStreamAttachment(new ByteArrayInputStream(originalBytes),                     ATTACHMENT_NAME, CONTENT_TYPE);

        // Create document revision with attachment
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setBody(DocumentBodyFactory.EMPTY);

        MutableDocumentRevision unsavedRev = builder.buildMutable();
        unsavedRev.attachments.put(streamAttachment.name, streamAttachment);

        DocumentRevision savedRevision = datastore.createDocumentFromRevision(unsavedRev);

        // Get saved attachment
        Attachment savedAtt = savedRevision.getAttachments().get(ATTACHMENT_NAME);
//        InputStream is = savedAtt.getInputStream();
//        byte[] actualBytes = new byte[(int)savedAtt.getSize()];
//        is.read(actualBytes);
        ByteArrayOutputStream bai = new ByteArrayOutputStream();
        IOUtils.copy(savedAtt.getInputStream(), bai);
        byte[] actualBytes = bai.toByteArray();


        // Compare bytes
        assertEquals(originalBytes.length, actualBytes.length);
        for (int i = 0; i < originalBytes.length; i++) {
            assertEquals(originalBytes[i], actualBytes[i]);
        }

        String actualBase64 = Base64.encodeToString(actualBytes, Base64.NO_WRAP);
        assertEquals(BASE64_ENCODED_IMAGE, actualBase64);

        manager.deleteDatastore(dbName);
        datastore = null;
    }

    @Test
    public void testUnsavedStreamAttachmentEncrypted() throws Exception {
        // Create encrypted test db
        dbName = "test" + System.currentTimeMillis();
        datastore = manager.openDatastore(dbName, keyProvider);

        // Decode Base64 image and get bytes
        byte[] originalBytes = Base64.decode(BASE64_ENCODED_IMAGE, Base64.NO_WRAP);
        UnsavedStreamAttachment streamAttachment = new UnsavedStreamAttachment(new ByteArrayInputStream(originalBytes),                     ATTACHMENT_NAME, CONTENT_TYPE);

        // Create document revision with attachment
        DocumentRevisionBuilder builder = new DocumentRevisionBuilder();
        builder.setBody(DocumentBodyFactory.EMPTY);

        MutableDocumentRevision unsavedRev = builder.buildMutable();
        unsavedRev.attachments.put(streamAttachment.name, streamAttachment);

        DocumentRevision savedRevision = datastore.createDocumentFromRevision(unsavedRev);

        // Get saved attachment
        Attachment savedAtt = savedRevision.getAttachments().get(ATTACHMENT_NAME);
//        InputStream is = savedAtt.getInputStream();
//        byte[] actualBytes = new byte[(int)savedAtt.getSize()];
//        is.read(actualBytes);
        ByteArrayOutputStream bai = new ByteArrayOutputStream();
        IOUtils.copy(savedAtt.getInputStream(), bai);
        byte[] actualBytes = bai.toByteArray();

        // Compare bytes
        assertEquals(originalBytes.length, actualBytes.length);
        for (int i = 0; i < originalBytes.length; i++) {
            assertEquals(originalBytes[i], actualBytes[i]);
        }

        String actualBase64 = Base64.encodeToString(actualBytes, Base64.NO_WRAP);
        assertEquals(BASE64_ENCODED_IMAGE, actualBase64);

        manager.deleteDatastore(dbName);
        datastore = null;
    }
}

