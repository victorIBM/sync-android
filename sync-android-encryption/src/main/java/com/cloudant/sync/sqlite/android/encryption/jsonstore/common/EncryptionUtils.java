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

/* Copyright (C) Worklight Ltd. 2006-2012.  All rights reserved. */

package com.cloudant.sync.sqlite.android.encryption.jsonstore.common;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.DialogInterface.OnClickListener;
import android.graphics.Bitmap;
import android.graphics.Matrix;
import android.graphics.drawable.BitmapDrawable;
import android.graphics.drawable.Drawable;
import android.os.Environment;
import android.os.StatFs;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

//import com.worklight.common.Logger;

/***
 * 
 * WLDroidGap should not be referenced here as this class is used for native android as well. Use Activity instead.
 * If you need to use WLDroidGap for hybrid case, implement it in WlUtils of com.worklight.common package.
 *
 */
public class EncryptionUtils {
	 //private static final Logger logger = Logger.getInstance(EncryptionUtils.class.getName());
     private static final Logger logger = Logger.getLogger(EncryptionUtils.class.getCanonicalName());

     public static final String LOG_CAT = "WL";
     public static final int ANDROID_BUFFER_8K = 8192;
     // SharedPreference name constant
     public static final String WL_PREFS = "WLPrefs";

     //Headers
     public static final String WL_CHALLENGE_DATA = "WL-Challenge-Data";
     public static final String WL_CHALLENGE_RESPONSE_DATA = "WL-Challenge-Response-Data";
     public static final String WL_INSTANCE_AUTH_ID = "WL-Instance-Id";

     public static final String WWW = "www";
     
     public static final String BUNDLE_BASENAME = "com.worklight.wlclient.messages";
     public static final String BUNDLE_RESOURCE = "/com/worklight/wlclient/messages";

     // keep track of which libs are already loaded so we don't process multiple calls
     // to loadLib method unecessarily.
     private static HashSet<String> LOADED_LIBS = new HashSet<String>();

	private static ResourceBundle bundle;

     public static Drawable scaleImage(Drawable drawable, float scaleWidth, float scaleHeight) {
          Drawable resizedDrawable = null;
          if (drawable != null) {
               Bitmap bitmapOrg = ((BitmapDrawable) drawable).getBitmap();
               int width = bitmapOrg.getWidth();
               int height = bitmapOrg.getHeight();
               // create a matrix for the manipulation
               Matrix matrix = new Matrix();
               // resize the bit map
               matrix.postScale(scaleWidth, scaleHeight);
               Bitmap resizedBitmap = Bitmap.createBitmap(bitmapOrg, 0, 0, width, height, matrix, true);
               // make a Drawable from Bitmap to allow to set the BitMap
               resizedDrawable = new BitmapDrawable(resizedBitmap);
          }
          return resizedDrawable;
     }

     public static int getResourceId(Context context, String resourceCategory, String resourceName) throws NoSuchResourceException {
          int resourceId = -1;
          try {
               @SuppressWarnings("rawtypes")
               Class[] classes = Class.forName(context.getPackageName() + ".R").getDeclaredClasses();
               for (int i = 0; i < classes.length; i++) {
                    if (classes[i].getSimpleName().equals(resourceCategory)) {
                         resourceId = classes[i].getField(resourceName).getInt(null);
                         break;
                    }
               }
          } catch (Exception e) {
               throw new NoSuchResourceException("Failed to find resource R." + resourceCategory + "." + resourceName, e);
          }
          return resourceId;
     }

     public static String getResourceString(String recourceName, Context context) {
    	 return getResourceString(recourceName, null, context);
     }
     
     public static String getResourceString(String recourceName, String argument, Context context) {
          @SuppressWarnings("rawtypes")
          // R$string class with reflection 
          Class rStringClass = null;

          try {
               if (rStringClass == null) {
                    rStringClass = Class.forName(context.getPackageName() + ".R$string");
               }               
               Integer resourceId = (Integer) rStringClass.getDeclaredField(recourceName).get(null);
               if (argument == null)
            	   return context.getResources ().getString (resourceId);
               else
            	   return context.getResources ().getString (resourceId, argument);
          } catch (Exception e) {
               logger.log(Level.SEVERE,e.getMessage(), e);
               return "";
          }
     }


     /**
      *  
      * @return the amount of free space on the Android device in Bytes
      */
     public static long getFreeSpaceOnDevice() {
          File path = Environment.getDataDirectory();
          StatFs stat = new StatFs(path.getPath());
          long blockSize = stat.getBlockSize();
          long availableBlocks = stat.getAvailableBlocks();
          long availableStorageInBytes = blockSize * availableBlocks;
          return availableStorageInBytes;
     }

     /**
      * copy source file to destination file.
      * If the destination file does not exist, it is created.
      * @param in The source file to be copied.
      * @param out The destination file to write to.
      */
     public static void copyFile(File in, File out) throws IOException {
          FileInputStream fis = new FileInputStream(in);
          if (!out.exists()) {
               if (in.isDirectory()) {
                    out.mkdirs();
               } else {
                    File parentDir = new File(out.getParent());
                    if (!parentDir.exists()) {
                         parentDir.mkdirs();
                    }
                    out.createNewFile();
               }
          }
          FileOutputStream fos = new FileOutputStream(out);
          try {
               byte[] buf = new byte[ANDROID_BUFFER_8K];
               int i = 0;
               while ((i = fis.read(buf)) != -1) {
                    fos.write(buf, 0, i);
               }
          } catch (IOException e) {
               throw e;
          } finally {
               fis.close();
               fos.close();
          }
     }

     /**
      * copy input stream to output stream
      * @param in The {@link InputStream} object to be copied from.
      * @param out The {@link OutputStream} object to write to.
      * @throws IOException in case copy fails.
      */
     public static void copyFile(InputStream in, OutputStream out) throws IOException {
          // 8k is the suggest buffer size in android, do not change this
          byte[] buffer = new byte[ANDROID_BUFFER_8K];
          int read;
          while ((read = in.read(buffer)) != -1) {
               out.write(buffer, 0, read);
          }
          out.flush();
     }


     /**
      * Delete a file or directory, including all its children.  The method name "deleteDirectory"
      * is retained for legacy callers, but can take a directory or file as a parameter.
      * 
      * @param fileOrDirectory The {@link File} object represents the directory to delete.
      * @return true if the directory was deleted, false otherwise.
      */
     public static boolean deleteDirectory(File fileOrDirectory) {
          if (fileOrDirectory.isDirectory()) {
               for (File child : fileOrDirectory.listFiles()) {
                    deleteDirectory(child);
               }
          }
          return fileOrDirectory.delete();
     }
     
     public static void calculateCheckSum(InputStream ios, Checksum checksum) {
         byte[] buffer = new byte[8192];

         int bytesRead = 0;

         try {
              while ((bytesRead = ios.read(buffer)) != -1) {
                   checksum.update(buffer, 0, bytesRead);
              }
         } catch (IOException e) {
              String errorMsg = "An error occurred while trying to read checksum from assets folder";
              logger.log(Level.SEVERE, errorMsg, e);
              //logger.error(errorMsg, e);
              throw new RuntimeException(errorMsg);
         } finally {
              try {
                   if (ios != null)
                        ios.close();
              } catch (IOException e) {
                   logger.log(Level.SEVERE, "Problem while trying to close InputStream", e);
                   //logger.debug("Problem while trying to close InputStream", e);
              }
         }
    }
     
     public static long computeChecksumOnExternal(String targetDir) {
         File targetFile = new File(targetDir);
         List<File> files = EncryptionUtils.getTree(targetFile);
         Collections.sort(files); // Sort files lexicographically
         Checksum checksum = new CRC32();
         for (File file : files) {
              try {
                   EncryptionUtils.calculateCheckSum(new FileInputStream(file), checksum);
              } catch (IOException e) {
                   //logger.error("Application failed to load, because checksum was not calculated for file " + file.getName() + " with " + e.getMessage(), e);
                   logger.log(Level.SEVERE, "Application failed to load, because checksum was not calculated for file " + file.getName() + " with " + e.getMessage(), e);
              }
         }

         return checksum.getValue();
    }

     /**
      * Convert InputStream data to String
      * @param is - The InputStream object to be converted
      * @return The converted string
      */
     public static String convertStreamToString(InputStream is) {
          BufferedReader reader = new BufferedReader(new InputStreamReader(is));
          StringBuilder sb = new StringBuilder();

          String line = null;
          try {
               while ((line = reader.readLine()) != null) {
                    sb.append(line + "\n");
               }
          } catch (IOException e) {
               throw new RuntimeException("Error reding input stream (" + is + ").", e);
          } finally {
               try {
                    is.close();
               } catch (IOException e) {
                    //logger.debug ("Failed to close input stream because " + e.getMessage (), e);
                    logger.log(Level.SEVERE,"Failed to close input stream because " + e.getMessage (), e);
               }
          }
          return sb.toString();
     }

     /** 
      * Convert GZipped InputStream data to String
      * @param is - The InputStream object to be converted
      * @return The converted string
      */
     public static String convertGZIPStreamToString(InputStream is) {
          StringBuilder sb = new StringBuilder();
          String line = null;
          try {
               GZIPInputStream gzipInputStream = new GZIPInputStream (is);
               BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream,Charset.forName("UTF-8")));

               while ((line = reader.readLine()) != null) {
                    sb.append(line + "\n");
               }
          } catch (IOException e) {
               throw new RuntimeException("Error reding input stream (" + is + ").", e);
          } finally {
               try {
                    is.close();
               } catch (IOException e) {
                    //logger.debug ("Failed to close input stream because " + e.getMessage (), e);
                    logger.log(Level.SEVERE,"Failed to close input stream because " + e.getMessage (), e);
               }
          }
          return sb.toString();
     }

     public static void showDialog(Context context, String title, String message, String buttonText) {
          showDialog(context, title, message, buttonText, new OnClickListener() {
               public void onClick(DialogInterface dialog, int which) {
                    dialog.dismiss();
               }
          });
     }

     public static void showDialog(final Context context, final String title, final String message, final String buttonText, final OnClickListener buttonClickListener) {
          ((Activity) context).runOnUiThread(new Runnable() {
               public void run() {
                    AlertDialog.Builder dlg = new AlertDialog.Builder(context);
                    dlg.setTitle(title);
                    dlg.setMessage(message);
                    dlg.setCancelable(false);
                    dlg.setPositiveButton(buttonText, buttonClickListener);
                    dlg.create();
                    dlg.show();
               }
          });	
     }

     public static void unpack(InputStream in, File targetDir) throws IOException {
          ZipInputStream zin = new ZipInputStream(in);

          ZipEntry entry;
          while ((entry = zin.getNextEntry()) != null) {
               String extractFilePath = entry.getName();
               if (extractFilePath.startsWith("/") || extractFilePath.startsWith("\\")) {
                    extractFilePath = extractFilePath.substring(1);
               }
               File extractFile = new File(targetDir.getPath() + File.separator + extractFilePath);
               if (entry.isDirectory()) {
                    if (!extractFile.exists()) {
                         extractFile.mkdirs();
                    }
                    continue;
               } else {
                    File parent = extractFile.getParentFile();
                    if (!parent.exists()) {
                         parent.mkdirs();
                    }
               }

               // if not directory instead of the previous check and continue
               OutputStream os = new BufferedOutputStream(new FileOutputStream(extractFile));
               EncryptionUtils.copyFile(zin, os);
               os.flush();
               os.close();
          }
     }



     public static List<File> getTree(File rootDir) {
          List<File> files = new ArrayList<File>();
          return getTree(rootDir, files);
     }

     private static List<File> getTree(File rootDir, List<File> files) {
          File[] filesToIterate = rootDir.listFiles();
          for (File file : filesToIterate) {
               if (file.isDirectory()) {
                    getTree(file, files); // Calls same method again.
               } else {
                    files.add(file);
               }
          }

          return files;
     }

     public static byte[] read(File file) throws IOException {

          byte[] buffer = new byte[(int) file.length()];
          InputStream ios = null;
          try {
               ios = new FileInputStream(file);
               if (ios.read(buffer) == -1) {
                    throw new IOException("EOF reached while trying to read the whole file");
               }
          } finally {
               try {
                    if (ios != null)
                         ios.close();
               } catch (IOException e) {
                    //
               }
          }

          return buffer;
     }



     // return the package name + application name
     public static String getFullAppName(Context context) {
          return context.getPackageName() + "." + context.getString(EncryptionUtils.getResourceId(context, "string", "app_name"));
     }

     /**
      * Convert JSON string to JSONObject
      * @param jsonString - The JSON String to be converted.
      * @return the converted JSON object.
      * @throws JSONException - in case convert fails.
      */
     public static final JSONObject convertStringToJSON(String jsonString) throws JSONException {
          int beginIndex = jsonString.indexOf("{");
          int endIndex = jsonString.lastIndexOf("}");
          
          if (beginIndex == -1 || endIndex == -1 || beginIndex > endIndex + 1) {
               String message = "Input string does not contain brackets, or input string is invalid. The string is: " + jsonString;
               //logger.debug (message);
               logger.log(Level.SEVERE,message);
               throw new JSONException(message);
          }
          
          String secureJSONString = jsonString.substring(beginIndex, endIndex + 1);
          JSONObject jsonObject = new JSONObject(secureJSONString);
          return jsonObject;
     }

     /**
      * Convert JSONArray to List<String>
      * @param jsonArray
      * @return - the converted  List<String>
      */
     public static final List<String> convertJSONArrayToList(JSONArray jsonArray) {
          List<String> listToReturn = new ArrayList<String>();
          for (int i = 0; i < jsonArray.length(); i++) {
               try {
                    listToReturn.add((String) jsonArray.get(i));
               } catch (JSONException e) {
                    throw new RuntimeException(e);
               }
          }
          return listToReturn;
     }

     public static final boolean isStringEmpty(String s) {
          return (s == null || s.length() == 0);
     }

     public static final byte[] hexStringToByteArray(String s) {
          int len = s.length();
          byte[] data = new byte[len / 2];
          for (int i = 0; i < len; i += 2) {
               data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
          }
          return data;
     }

     public static final String byteArrayToHexString(byte[] bytes) {  
          StringBuilder sb = new StringBuilder(bytes.length * 2);  

          Formatter formatter = new Formatter(sb);  
          for (byte b : bytes) {  
               formatter.format("%02x", b);  
          }  
          formatter.close ();
          return sb.toString();  
     }


     /**
      * This method assumes it will find the library at:
      *     files/featurelibs/{arch}/{library}.zip
      *     
      * It will unzip the library to the root folder, and delete the original,
      * then see if any other architecture folders exist and delete them since
      * they will never be used on this architecture.
      * 
      * @param ctx
      * @param library example "libcrypto.so.1.0.0"
      * 
      * 
      */
     public static final synchronized void loadLib(Context ctx, String library) {
          
          // keep track of which libs are already loaded, so we don't process multiple calls for the same lib unnecessarily
          // Notice we use a static.  This means calls to loadLib for the same 'library' parameter will be processed
          // only upon app startup, not app foreground.  We want to keep the behavior for cases where the native app has been
          // updated (through the Play Store, for example) and the target .so file needs to be replaced.
          
          if (!LOADED_LIBS.contains(library)) {

               // we only support "armeabi" and "x86"
               final String ARMEABI = "armeabi";
               final String X86 = "x86";

               String arch = System.getProperty("os.arch");  // the architecture we're running on
               String nonArch = null;  // the architecture we are NOT on
               if (arch != null && arch.toLowerCase().startsWith("i")) {  // i686
                    arch = X86;
                    nonArch = ARMEABI;
               } else {
                    arch = ARMEABI;
                    nonArch = X86;
               }

               final String libPath = "featurelibs" + File.separator +  arch + File.separator + library;
               File sourceLocation = new File(ctx.getFilesDir(), libPath + ".zip");

               // recursively delete the architecture folder that will never be used:
               File nonArchStorage = new File(ctx.getFilesDir(), "featurelibs" + File.separator + nonArch);
               deleteDirectory(nonArchStorage);

               File targetFile = new File(ctx.getFilesDir(), library);

               // delete the target
               targetFile.delete();

               //logger.debug("Extracting zip file: " + libPath);
               logger.log(Level.SEVERE,"Extracting zip file: " + libPath);
               try{
                    InputStream istr = ctx.getAssets().open(libPath + ".zip");
                    unpack(istr, targetFile.getParentFile());
               }
               catch(IOException e){
            	   e.printStackTrace();
                    logger.log(Level.SEVERE, "Error extracting zip file: " + e.getMessage());
                    //logger.debug("Error extracting zip file: " + e.getMessage());
               }
               logger.log(Level.SEVERE, "Loading library using System.load: " + targetFile
                       .getAbsolutePath());
               //logger.debug("Loading library using System.load: " + targetFile.getAbsolutePath());

               // delete the original zip, which is now extracted:
               //sourceLocation.delete();

               System.load(targetFile.getAbsolutePath());
               
               LOADED_LIBS.add (library);
          }
     }

     

     
     public static void clearState() {
    	 bundle = null;
     }
     
     /**
      * @return SDK version of android
      */
     public static int getSDKVersion(){
         return android.os.Build.VERSION.SDK_INT;
     }
}
