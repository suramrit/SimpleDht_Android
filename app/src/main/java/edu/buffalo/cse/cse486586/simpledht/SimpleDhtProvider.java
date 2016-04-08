package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    public String my_hash;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    public String myPort;
    private ArrayList<String> node_list= new ArrayList<String>();
    private ArrayList<String> node_hash_list= new ArrayList<String>();
    private ArrayList<String> local_key_list = new ArrayList<String>();
    private Map<String, String> node_map = new TreeMap<String, String>();
    static final int SERVER_PORT = 10000;
    static final int REQUEST_PORT = 11111;
    private String my_succ = new String();
    private String my_pred = new String();
    private String succ_node = new String();
    private String pred_node = new String();
    private String min_hash = new String();
    private String max_hash = new String();
    private String min_hash_node = new String();
    private String max_hash_node = new String();
    private Boolean stand_alone =false;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        //Boolean bool = false;
        boolean done = false;
       // for (String key : local_key_list) {
            try {
                // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                String filename = selection;
                getContext().deleteFile(selection);
            } catch (Exception e) {
                System.out.println("FILE Delete FAILED");
            }
            Log.v("Deleted-", selection+"..."+done);
       // }



        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            Thread.sleep(200);
        }
        catch(InterruptedException e){
            Log.e(TAG,"Problem in insert sleep"+e);
        }
        Context context = getContext();
        Set<Map.Entry<String,Object>> s=values.valueSet();
        Iterator itr = s.iterator();
        String val = new String();
        String key = new String();
        String KEY_FIELD = "key";
        String VALUE_FIELD = "value";
        int i = 0;
        while(itr.hasNext())
        {
            Map.Entry me = (Map.Entry)itr.next();
            Object value =  me.getValue();
            if(i==0){
                val = value.toString();
                i++;
            }
            else
            {
                key = value.toString();
            }
        }

        //got the key,value pair from insert request.. check node..
        try{

            if (succ_node.compareTo(myPort)==0 && pred_node.compareTo(myPort)==0 ){
                //stand alone case
                Log.e(TAG,"Stand Alone case");
                ContentValues val_insrt = new ContentValues();
                val_insrt.put(VALUE_FIELD,val);
                val_insrt.put(KEY_FIELD, key);
                local_key_list.add(key);

                System.out.println("Inserting values :key-- " + key + " val:" + val);
                String filename = key;
                FileOutputStream outputStream;

                try {
                    outputStream = context.openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                }
                catch(Exception e){
                    System.out.println("FILE WRITE FAILED");
                }

            }
            else if(my_hash.compareTo(min_hash)==0 && ( max_hash.compareTo(genHash(key))<0 || my_hash.compareTo(genHash(key))>0)){
                //I will store it
                Log.e(TAG,"MIN CASE: My Hash:"+my_hash+"..key hash:"+genHash(key)+"..comp:"+my_hash.compareTo(genHash(key)));
                ContentValues val_insrt = new ContentValues();
                val_insrt.put(VALUE_FIELD,val);
                val_insrt.put(KEY_FIELD, key);
                local_key_list.add(key);

                System.out.println("Inserting values :key-- " + key + " val:" + val);
                String filename = key;
                FileOutputStream outputStream;

                try {
                    outputStream = context.openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                }
                catch(Exception e){
                    System.out.println("FILE WRITE FAILED");
                }

            }
            else if(genHash(key).compareTo(my_hash)<0 && genHash(key).compareTo(my_pred)>0){
                //I will store it..
                Log.e(TAG,"My Hash:"+my_hash+"..key hash:"+genHash(key)+"..comp:"+my_hash.compareTo(genHash(key)));
                ContentValues val_insrt = new ContentValues();
                val_insrt.put(VALUE_FIELD,val);
                val_insrt.put(KEY_FIELD, key);
                local_key_list.add(key);

                System.out.println("Extracted Values:key-- " + key + " val:" + val);
                String filename = key;
                FileOutputStream outputStream;

                try {
                    outputStream = context.openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                }
                catch(Exception e){
                    System.out.println("FILE WRITE FAILED");
                }
            }
            else{
                //pass to successor
                Log.e(TAG,"passing forward...");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succ_node)); //
                    PrintWriter out =
                            new PrintWriter(socket.getOutputStream(), true);
                    ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                    //obj_out_old.flush();
                    String req_type = "1";
                    Log.e(TAG, "Passing insert request: " + key + ".." + val);
                    Log.e(TAG,"comp:"+my_hash.compareTo(genHash(key))+"to:" + succ_node);
                    obj_out.writeObject(req_type);
                    obj_out.writeObject(key);
                    obj_out.writeObject(val);
                    //socket.close();
                }
                catch(IOException e){
                    Log.e(TAG,"Error forwarding insert:"+e);
                }
                //new forward_insert_request().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ_node, key, val); // edit this...
                // change code for server to handle insert requests..
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG,"Cannot hash insert key");
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //redundant ....but verifying
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e(TAG, "My PORT is: " + myPort);
        try {
            my_hash = genHash(Integer.toString(Integer.parseInt(myPort) / 2));
            System.out.println("My Hash value is:" + my_hash);

            if((Integer.parseInt(myPort) / 2) == 5554)
            {
                System.out.println("I am master");
                //add self as the first node
                //start handler
                my_succ = my_hash;
                my_pred = my_hash;
                succ_node = myPort;
                pred_node = myPort;
                node_list.add(myPort);
                node_hash_list.add(my_hash);
                node_map.put(my_hash, myPort);
                stand_alone = true;

                try {
                    ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                    new MasterServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,serverSocket);
                    //CORRECT ???
                    //ServerSocket requestSocket = new ServerSocket(SERVER_PORT);
                    //new RequestServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestSocket);
                }
                catch(IOException e) {
                    Log.e(TAG, "Can't create a ServerSocket");
                }
            }


            else {
                //query master for pred and succ nodes..
                System.out.println("I am slave");
                my_succ = my_hash;
                my_pred = my_hash;
                succ_node = myPort;
                pred_node = myPort;
                //communicate with master for predd.. succ..
                try {
               new inform_master().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, my_hash); // just use a socket and close

                // set server to recieve sucessor update .. assuming no change in pred..

                    //also handles dht requests..
                    ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                    new RequestServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, serverSocket);
                    //ServerSocket requestSocket = new ServerSocket(SERVER_PORT);
                    //new RequestServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestSocket);
                }
                catch(IOException e) {
                    Log.e(TAG, "Can't create a Update Server");
                }
            }

        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG,"Hash generation for port no failed!");
        }
        //check here if you are 5554--be master.. else query master for your predd and succ nodes..
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        try {
            Thread.sleep(200);
        }
        catch(InterruptedException e){
            Log.e(TAG,"Problem in the query sleep"+e);
        }
        if(succ_node.compareTo(myPort)==0 && pred_node.compareTo(myPort)==0)
            stand_alone = true;
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
        Context context = getContext();
        System.out.println("Key recvd is: " + selection);
        System.out.println("QUERY: stand alon is true?" + stand_alone);
        // query is implemented as: mContentResolver.query(mUri, null, key, null, null);
        //read the file using 'key' and then add a row to cursor and return
        //then check -- DONE
        try {
            if ((stand_alone == true && (selection.compareTo("*") == 0 || selection.compareTo("@") == 0))
                    || selection.compareTo("@") == 0) {
                //check if stand alone case
                for (String key : local_key_list) {
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        String filename = key;
                        FileInputStream inputStream;
                        inputStream = context.openFileInput(filename);
                        int avail = inputStream.available();
                        byte[] value = new byte[avail];
                        inputStream.read(value);
                        inputStream.close();
                        String read_val = new String(value, "UTF-8");
                        //reader.close();
                        System.out.println("The value read is:" + read_val);
                        //Can correctly read from file: Check how to send it as cursor
                        Object[] ret_cursor = {filename, read_val};
                        matrixCursor.addRow(ret_cursor);
                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                    Log.v("query", selection);


                }
            }
            else if(stand_alone==false && selection.compareTo("*") == 0){
                //global all query...
                try {
                    Thread.sleep(5000);
                }
                catch(InterruptedException e){
                    Log.e(TAG,"Problem in insert sleep"+e);
                }
                Map<String,String> global_vals = new HashMap<String, String>();
                for(String key : local_key_list){
                    //get local stored values
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        String filename = key;
                        FileInputStream inputStream;
                        inputStream = context.openFileInput(filename);
                        int avail = inputStream.available();
                        byte[] value = new byte[avail];
                        inputStream.read(value);
                        inputStream.close();
                        String read_val = new String(value, "UTF-8");
                        //reader.close();
                        System.out.println("The value read is:" + read_val);
                        global_vals.put(key,read_val);
                        //Can correctly read from file: Check how to send it as cursor

                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                }
                Log.e(TAG, "My own rows: " + global_vals.size());
                for(String node : node_list){
                    if(node.compareTo(myPort)!=0) {
                        try {

                            Log.e(TAG, "propagating query * global to:" + node);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(node)); //
                            ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                            //obj_out_old.flush();
                            String req_type = "-1";
                            obj_out.writeObject(req_type);
                            ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                            Object returned_keys = obj_inp.readObject();
                            Map<String,String> retKey = (HashMap<String,String>) returned_keys;
                            global_vals.putAll(retKey);
                        }
                        catch(IOException e){
                            Log.e(TAG,"Error sending global * query"+e);
                        }
                        catch(ClassNotFoundException e){
                            Log.e(TAG,"Error reading global * query"+e);
                        }
                    }
                }

                for(String key : global_vals.keySet()){
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        Log.e(TAG,"Total rows: "+global_vals.size());
                        Object[] ret_cursor = {key,global_vals.get(key)};
                        matrixCursor.addRow(ret_cursor);
                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                    Log.v("query", key);

                }

            }
            else if ((my_hash.compareTo(genHash(selection)) > 0 && my_pred.compareTo(genHash(selection)) < 0)
                    || (my_hash.compareTo(min_hash) == 0 && max_hash.compareTo(genHash(selection)) < 0)
                    || (my_hash.compareTo(min_hash) == 0 && my_hash.compareTo(genHash(selection)) > 0))
                {
                // key is in my LFS
                try {
                    // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                    String filename = selection;
                    FileInputStream inputStream;
                    inputStream = context.openFileInput(filename);
                    int avail = inputStream.available();
                    byte[] value = new byte[avail];
                    inputStream.read(value);
                    inputStream.close();
                    String read_val = new String(value, "UTF-8");
                    //reader.close();
                    System.out.println("The value read is:" + read_val);
                    //Can correctly read from file: Check how to send it as cursor
                    Object[] ret_cursor = {filename, read_val};
                    matrixCursor.addRow(ret_cursor);
                } catch (Exception e) {
                    System.out.println("FILE OPEN FAILED");
                }

                Log.v("query", selection);


            } else if(stand_alone != true) {

                //pass to successor
                Log.e(TAG,"passing query forward...");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succ_node)); //
                    ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                    //obj_out_old.flush();
                    String req_type = "2";
                    Log.e(TAG, "Passing query request: " + selection);
                    Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + succ_node);
                    obj_out.writeObject(req_type);
                    obj_out.writeObject(selection);
                    //socket.close();\
                    ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                    Object returnkey = obj_inp.readObject();
                    String retKey = (String) returnkey;
                    Object returnvalue = obj_inp.readObject();
                    String retVal = (String) returnvalue;
                    Object[] ret_cursor = {retKey, retVal};
                    matrixCursor.addRow(ret_cursor);
                }
                catch(IOException e){
                    Log.e(TAG,"Error forwarding query:"+e);
                }
                catch (ClassNotFoundException e){
                    Log.e(TAG,"error reading result of passed query"+e);
                }

            }
            else if(stand_alone == true){
                try {
                    // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                    String filename = selection;
                    FileInputStream inputStream;
                    inputStream = context.openFileInput(filename);
                    int avail = inputStream.available();
                    byte[] value = new byte[avail];
                    inputStream.read(value);
                    inputStream.close();
                    String read_val = new String(value, "UTF-8");
                    //reader.close();
                    System.out.println("The value read is:" + read_val);
                    //Can correctly read from file: Check how to send it as cursor
                    Object[] ret_cursor = {filename, read_val};
                    matrixCursor.addRow(ret_cursor);
                } catch (Exception e) {
                    System.out.println("FILE OPEN FAILED");
                }

                Log.v("query", selection);


            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG,"Error gen hash for selection in query"+e);
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b); //lexographic comparison
        }
        return formatter.toString();
    }

    //method for update predd-succ for a node....
    private void update_pred_succ(){
        try {
            min_hash = genHash(Integer.toString(Integer.parseInt(node_list.get(0))/2));
            max_hash = genHash(Integer.toString(Integer.parseInt(node_list.get(0))/2));
            max_hash_node = new String(); //
            min_hash_node = new String(); //
            for (String node : node_list) {
                String node_hash = genHash(Integer.toString(Integer.parseInt(node) / 2));
                if (min_hash.compareTo(node_hash) > 0) {
                    min_hash = node_hash;
                    min_hash_node = node;
                }
                else if (max_hash.compareTo(node_hash) < 0) {
                    max_hash = node_hash;
                    max_hash_node = node;
                }
                Log.e(TAG, "Min:" + min_hash + "max:" + max_hash);
            }
            for (String node : node_list) {
                try {
                    if (node.compareTo(myPort) != 0) {
                        System.out.println("Node is:" + node);
                        String node_hash = genHash(Integer.toString(Integer.parseInt(node) / 2));
                        if (node_list.size() == 2 && (Integer.parseInt(myPort) / 2) != 5554) {
                            my_pred = node_hash;
                            my_succ = node_hash;
                            pred_node = node;
                            succ_node = node;
                            Log.e(TAG, "My new succ+pred:" + node_hash);
                        } //first case
                        else if(my_hash.compareTo(node_hash)<0 && my_hash.compareTo(my_succ)==0){
                            my_succ = node_hash;
                            succ_node = node;
                        }
                        else if(my_hash.compareTo(node_hash)>0 && my_hash.compareTo(my_pred)==0){
                            my_pred = node_hash;
                            pred_node = node;
                        }
                        else if(my_succ.compareTo(min_hash)==0 && my_hash.compareTo(node_hash)<0){
                            my_succ = node_hash;
                            succ_node = node;
                        }
                        else if(my_pred.compareTo(max_hash)==0 && min_hash.compareTo(node_hash)==0){
                            my_pred = node_hash;
                            pred_node = node;
                        }
                        else if(my_succ.compareTo(node_hash)>0 && my_hash.compareTo(node_hash)<0){
                            my_succ = node_hash;
                            succ_node = node;
                        }
                        else if(my_pred.compareTo(node_hash)<0 && my_hash.compareTo(node_hash)>0){
                            my_pred = node_hash;
                            pred_node = node;
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "Failed to gen hash:" + e);
                }
            }
            if(node_list.size() !=2 ) {
                if (my_hash.compareTo(max_hash) == 0) {
                    my_succ = min_hash;
                    succ_node = min_hash_node;
                } else if (my_hash.compareTo(min_hash) == 0) {
                    my_pred = max_hash;
                    pred_node = max_hash_node;
                }
            }
            Log.e(TAG, "my updated predd is:" + my_pred);
            Log.e(TAG, "my updated succ is:" + my_succ);
            Log.e(TAG, "my updated predd node is:" + pred_node);
            Log.e(TAG, "my updated succ is:" + succ_node);
        }
        catch(NoSuchAlgorithmException e){
            Log.e(TAG,"node list error1:"+e);
        }

        }



    private class MasterServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            System.out.println("Starting Master Server");
            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    //serverSocket.setSoTimeout(2000);
                    //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true); //trying 2-way
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    ObjectInputStream obj_inp = new ObjectInputStream(clientSocket.getInputStream());
                    Object req_obj = obj_inp.readObject();
                    String req_type = (String) req_obj;
                    if (Integer.parseInt(req_type) == 0) { // A new Join
                        Object port_obj = obj_inp.readObject();
                        Object hash_obj = obj_inp.readObject();
                        String new_join_port = (String)port_obj;
                        String new_join_hash = (String)hash_obj;
                        Log.e(TAG, "New Joinees Port:" + new_join_port + "..hash:" + new_join_hash);
                        stand_alone = false;
                        //update for both the node at end of list and the new recruit
                        node_list.add(new_join_port);
                        node_hash_list.add(new_join_hash);
                        node_map.put(new_join_hash, new_join_port);
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(node_list); // doesnt seem to be working...
                        if (node_list.size() == 2) {  //This is my successor+predecessor
                            //update_pred_succ(node_map);
                            //ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                            //obj_out.writeObject(node_list);
                            my_pred = new_join_hash;
                            my_succ = new_join_hash;
                            succ_node = new_join_port;
                            pred_node = new_join_port;
                            Log.e(TAG, "my predd is:" + my_pred);
                            Log.e(TAG, "my succ is:" + my_succ);
                        } else {
                            Log.e(TAG,"another new joinee");
                            //String last_join = node_list.get(node_list.size() - 2);
                            //inform about the update in sucessor to this node....
                            for(String node : node_list ) {
                                if(node.compareTo(myPort)!=0 && node.compareTo(new_join_port)!=0) { //inform others of a new join..
                                    Socket socket_old = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(node));
                                    //PrintWriter out_old =
                                            //new PrintWriter(socket_old.getOutputStream(), true);
                                    //out_old.flush();
                                    //out_old.println("0");
                                    ObjectOutputStream obj_out_old = new ObjectOutputStream(socket_old.getOutputStream());
                                   //obj_out_old.flush();
                                    String op = "0";
                                    obj_out_old.writeObject(op);
                                    obj_out_old.writeObject(node_list);
                                    //socket_old.close();
                                }
                            }
                            Log.e(TAG, "Olds Predd-Succ Updated");
                            update_pred_succ();
                            Log.e(TAG, "My Predd-Succ updated..");

                        }
                    }
                    else if(Integer.parseInt(req_type) == 1) { //insert request
                        Object key_obj = obj_inp.readObject();
                        String key = (String) key_obj;
                        Object val_obj = obj_inp.readObject();
                        String val = (String) val_obj;

                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        ContentValues mContentValues = new ContentValues();
                        ContentResolver mContentResolver = getContext().getContentResolver();

                        mContentValues.put("key", key);
                        mContentValues.put("value", val);
                        mContentResolver.insert(mUri, mContentValues);
                    }
                    else if(Integer.parseInt(req_type) == 2){ //Query Request
                        Object key_obj = obj_inp.readObject();
                        String selection = (String) key_obj;
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        ContentValues mContentValues = new ContentValues();
                        ContentResolver mContentResolver = getContext().getContentResolver();
                        Cursor resultCursor = mContentResolver.query(mUri, null,
                                selection, null, null);
                        int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                        int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                        resultCursor.moveToFirst();
                        String returnKey = resultCursor.getString(keyIndex);
                        String returnValue = resultCursor.getString(valueIndex);
                        resultCursor.close();
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(returnKey);
                        obj_out.writeObject(returnValue);
                    }
                    else if(Integer.parseInt(req_type) == -1){ //A global * query
                        Map<String,String> my_local_vals = new HashMap<String, String>();
                        for(String key : local_key_list){
                            //get local stored values
                            try {
                                // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                                String filename = key;
                                FileInputStream inputStream;
                                inputStream = getContext().openFileInput(filename);
                                int avail = inputStream.available();
                                byte[] value = new byte[avail];
                                inputStream.read(value);
                                inputStream.close();
                                String read_val = new String(value, "UTF-8");
                                //reader.close();
                                System.out.println("The value read is:" + read_val);
                                my_local_vals.put(key, read_val);
                                //Can correctly read from file: Check how to send it as cursor
                            } catch (Exception e) {
                                System.out.println("FILE OPEN FAILED");
                            }

                        }
                        Log.e(TAG,"Returning rows: "+my_local_vals.size());
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(my_local_vals);
                    }
                }
            }
            catch(IOException e){

                Log.e(TAG, "Cant create Master Server"+e);
            }
            catch(ClassNotFoundException e){
                Log.e(TAG,"error reading object from object stream"+e);
            }
            return null;
        }
    }

    private class inform_master extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs){
            try {
                //Thread.sleep(Integer.parseInt(myPort)/6);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("11108"));
                //PrintWriter out =
                        //new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                String req_type = "0";
                obj_out.writeObject(req_type);
                obj_out.writeObject(myPort);
                obj_out.writeObject(my_hash);
                //BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                Object new_list = obj_inp.readObject();
                node_list = (ArrayList) new_list;
                for (String node : node_list){
                    System.out.println("The nodes currently joined are:"+node+"..");
                }
                update_pred_succ();
                //my_pred = in.readLine();
                //my_succ = in.readLine();
                //socket.close();
                Log.e(TAG,"my predd is:" + my_pred);
                Log.e(TAG,"my succ is:"+my_succ);
            }
            catch (IOException e){
                Log.e(TAG,"Could not send information to master"+ e);
                stand_alone = true;
                Log.e(TAG,"Stand Alone Instance"+ stand_alone);
                my_pred = my_hash;
                my_succ = my_hash;
                pred_node = myPort;
                succ_node = myPort;
            }
            catch (ClassNotFoundException e){
                Log.e(TAG,"Error in reading object"+ e);
            }
            //catch (InterruptedException e){
               // Log.e(TAG,"Error Thread Sleep"+ e);
            //}


           return null;
        }



    }

    private class RequestServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            System.out.println("Starting Request Server");
            try {
                while (true){

                    Socket clientSocket = serverSocket.accept();
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true); //trying 2-way
                    //BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    ObjectInputStream obj_inp = new ObjectInputStream(clientSocket.getInputStream());
                    Object req_type = obj_inp.readObject();
                    String req = (String) req_type;
                    System.out.println("Read:" + req);
                    if (Integer.parseInt(req) == 0) {
                    //ObjectInputStream obj_inp = new ObjectInputStream(clientSocket.getInputStream());
                    Object new_list = obj_inp.readObject();
                    node_list = (ArrayList) new_list;
                    for (String node : node_list) {
                        System.out.println("Updated node list:" + node + "..");
                        }
                    update_pred_succ();
                    }

                    if(Integer.parseInt(req) == 1){
                        Object key_obj = obj_inp.readObject();
                        String key = (String) key_obj;
                        Object val_obj = obj_inp.readObject();
                        String val = (String) val_obj;

                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        ContentValues mContentValues = new ContentValues();
                        ContentResolver mContentResolver = getContext().getContentResolver();

                        mContentValues.put("key", key);
                        mContentValues.put("value", val);
                        mContentResolver.insert(mUri, mContentValues);

                    }
                    if(Integer.parseInt(req) == 2){
                        Object key_obj = obj_inp.readObject();
                        String selection = (String) key_obj;
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        ContentValues mContentValues = new ContentValues();
                        ContentResolver mContentResolver = getContext().getContentResolver();
                        Cursor resultCursor = mContentResolver.query(mUri, null,
                                selection, null, null);
                        int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                        int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                        resultCursor.moveToFirst();
                        String returnKey = resultCursor.getString(keyIndex);
                        String returnValue = resultCursor.getString(valueIndex);
                        resultCursor.close();
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(returnKey);
                        obj_out.writeObject(returnValue);

                    }
                    if(Integer.parseInt(req) == -1){ //A global * query
                        Map<String,String> my_local_vals = new HashMap<String, String>();
                        for(String key : local_key_list){
                            //get local stored values
                            try {
                                // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                                String filename = key;
                                FileInputStream inputStream;
                                inputStream = getContext().openFileInput(filename);
                                int avail = inputStream.available();
                                byte[] value = new byte[avail];
                                inputStream.read(value);
                                inputStream.close();
                                String read_val = new String(value, "UTF-8");
                                //reader.close();
                                System.out.println("The value read is:" + read_val);
                                my_local_vals.put(key, read_val);
                                //Can correctly read from file: Check how to send it as cursor
                            } catch (Exception e) {
                                System.out.println("FILE OPEN FAILED");
                            }

                        }
                        Log.e(TAG,"Returning rows: "+my_local_vals.size());
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(my_local_vals);
                    }
                }
            }
            catch (IOException e){
                Log.e(TAG,"Master Reply Failed:"+e);
            }
            catch (ClassNotFoundException e){
                Log.e(TAG, "error in reading object"+e);
            }


            return null;
        }


    }

    private class forward_insert_request extends AsyncTask<String, Void, Void>{
        @Override
        protected Void doInBackground(String... msg){
            try {
                System.out.println("Hello....");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msg[0])); //
                PrintWriter out =
                        new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                //obj_out_old.flush();
                String req_type = "1";
                Log.e(TAG,"Passing insert request: "+msg[1]+".."+msg[2]+"to:"+msg[0]);
                obj_out.writeObject(req_type);
                obj_out.writeObject(msg[1]);
                obj_out.writeObject(msg[2]);
                socket.close();
            }
            catch (IOException e){
                Log.e(TAG,"Could not forward request:"+e);
            }
            return null;
        }
    }



}


