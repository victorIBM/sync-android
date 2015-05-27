/**
 * Copyright (c) 2014 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.datastore;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An attachment which has been been copied to a temporary location and had its sha1 calculated,
 * prior to being added to the datastore.
 *
 * In most cases, this class will only be used by the AttachmentManager and BasicDatastore classes.
 */
public class PreparedAttachment {

    private Logger logger = Logger.getLogger(PreparedAttachment.class.getCanonicalName());
    /**
     * Prepare an attachment by copying it to a temp location and calculating its sha1.
     *
     * @param attachment The attachment to prepare
     * @param attachmentsDir The 'BLOB store' or location where attachments are stored for this database
     * @throws AttachmentNotSavedException
     */
    public PreparedAttachment(Attachment attachment,
                              String attachmentsDir) throws AttachmentException {
        this.attachment = attachment;
        this.tempFile = new File(attachmentsDir, "temp" + UUID.randomUUID());
        InputStream attachmentInStream = null;
        OutputStream tempFileOutStream = null;
        MessageDigest sha1 = null;
        try {
            attachmentInStream = attachment.getInputStream();

            //Use FileUtils to create folder structure and file if necessary for output stream
            tempFileOutStream = FileUtils.openOutputStream(this.tempFile);

            sha1 = MessageDigest.getInstance("SHA-1");
            int bufSiz = 1024;
            byte buf[] = new byte[bufSiz];
            int bytesRead;
            int totalRead = 0;
            while ((bytesRead = attachmentInStream.read(buf)) != -1) {
                sha1.update(buf, 0, bytesRead);
                tempFileOutStream.write(buf, 0, bytesRead);
                totalRead += bytesRead;
            }

            //Set attachment length from bytes read in input stream
            this.length = totalRead;
        } catch (IOException e) {
            logger.log(Level.WARNING,"Problem reading input or output stream ",e);
            throw new AttachmentNotSavedException(e);
        } catch (NoSuchAlgorithmException e) {
            logger.log(Level.WARNING,"Problem calculating SHA1 for attachment stream ",e);
            throw new AttachmentNotSavedException(e);
        } finally {
            //Ensure the attachment input stream and file output stream is closed after calculating the hash
            IOUtils.closeQuietly(attachmentInStream);
            IOUtils.closeQuietly(tempFileOutStream);
        }
        this.sha1 = sha1.digest();
    }

    public int getLength() {
        return length;
    }

    public final Attachment attachment;
    public final File tempFile;
    public final byte[] sha1;
    private int length;
}



