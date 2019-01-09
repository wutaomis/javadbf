package com.linuxense.javadbf;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class DBFRandomAccess extends DBFBase implements Closeable {

    private static final long MILLISECS_PER_DAY = 24*60*60*1000;
    private static final long TIME_MILLIS_1_1_4713_BC = -210866803200000L;

    private DBFHeader header;
    private int recordCount = 0;
    private boolean closed = false;
    private boolean showDeletedRows;
    private boolean trimRightSpaces = true;
    private Map<String, Integer> mapFieldNames = new HashMap<String, Integer>();

    private RandomAccessFile raf;
    private DBFMemoFile memoFile = null;



    /**
     * Creates a DBFRandomAccess which can both read and write an existing DBF file.
     *
     * @param dbfFile The file passed in shouls be a valid DBF file.
     * @exception DBFException
     *                if the passed in file does exist but not a valid DBF file,
     *                or if an IO error occurs.
     */
    public DBFRandomAccess(File dbfFile){
        this(dbfFile, null, false);
    }

    /**
     * Creates a DBFRandomAccess which can both read and write an existing DBF file.
     *
     * @param dbfFile The file passed in shouls be a valid DBF file.
     * @param charset The charset used to encode field name and field contents
     * @param showDeletedRows whether to display records marked deleted
     * @exception DBFException
     *                if the passed in file does exist but not a valid DBF file,
     *                or if an IO error occurs.
     */
    public DBFRandomAccess(File dbfFile, Charset charset, boolean showDeletedRows){
        super();
        try {
            this.showDeletedRows = showDeletedRows;
            this.raf = new RandomAccessFile(dbfFile, "rw");
            this.header = new DBFHeader();

            /*
             * before proceeding check whether the passed in File object is an
             * empty/non-existent file or not.
             */
            if (dbfFile.length() == 0) {
                if (charset != null) {
                    if (DBFCharsetHelper.getDBFCodeForCharset(charset) == 0) {
                        throw new DBFException("Unssuported charset " + charset);
                    }
                    this.header.setUsedCharset(charset);
                }
                else {
                    this.header.setUsedCharset(StandardCharsets.ISO_8859_1);
                }
                return;
            }

            // position file pointer will be at the end of header after this
            this.header.read(this.raf, charset, false);

        } catch (FileNotFoundException e) {
            throw new DBFException("Specified file is not found. " + e.getMessage(), e);
        } catch (IOException e) {
            throw new DBFException("Read DBF header Exception. " + e.getMessage());
        }

        this.recordCount = this.header.numberOfRecords;
        this.mapFieldNames = createMapFieldNames(this.header.userFieldArray);
    }

    /**
     * Creates a DBFRandomAccess which can both read and write a DBF file.
     *
     * @param dbfFileName The file name passed in.
     * @param charset The charset used to encode field name and field contents
     * @param showDeletedRows whether to display records marked deleted
     * @exception DBFException
     *                if the passed in file does exist but not a valid DBF file,
     *                or if an IO error occurs.
     */
    public DBFRandomAccess(String dbfFileName, Charset charset, boolean showDeletedRows) {
        this(new File(dbfFileName), charset, showDeletedRows);
    }

    /**
     * Sets fields definition
     * @param fields fields definition
     */
    public void setFields(DBFField[] fields) {
        if (this.closed) {
            throw new IllegalStateException("You can not set fields to a closed DBFWriter");
        }
        if (this.header.fieldArray != null) {
            throw new DBFException("Fields has already been set");
        }
        if (fields == null || fields.length == 0) {
            throw new DBFException("Should have at least one field");
        }
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] == null) {
                throw new DBFException("Field " + i + " is null");
            }
        }
        this.header.fieldArray = new DBFField[fields.length];
        for (int i = 0; i < fields.length; i++) {
            this.header.fieldArray[i] = new DBFField(fields[i]);
        }
        try {
            if (this.raf != null && this.raf.length() == 0) {
                // this is a new/non-existent file. So write header before proceeding
                this.header.write(this.raf);
            }
        } catch (IOException e) {
            throw new DBFException("Error accesing file:" + e.getMessage(), e);
        }
    }

    /**
     * Returns the asked Field. In case of an invalid index, it returns a
     * ArrayIndexOutofboundsException.
     *
     * @param index Index of the field. Index of the first field is zero.
     * @return Field definition for selected field
     */
    public DBFField getField(int index) {
        if (index < 0 || index >= this.header.userFieldArray.length) {
            throw new IllegalArgumentException("Invalid index field: (" + index+"). Valid range is 0 to " +
                    (this.header.userFieldArray.length - 1));
        }
        return new DBFField(this.header.userFieldArray[index]);
    }

    /**
     * Returns the number of field in the DBF.
     * @return number of fields in the DBF file
     */
    public int getFieldCount() {
        return this.header.userFieldArray.length;
    }

    /**
     * Gets the date the file was modified
     * @return The date de file was created
     */
    public Date getLastModificationDate() {
        if (this.header.getYear() == 0 || this.header.getMonth() == 0 || this.header.getDay() == 0){
            return null;
        }
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.set(this.header.getYear(), this.header.getMonth(), this.header.getDay(),
                    0, 0, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return calendar.getTime();
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     Returns the number of records in the DBF. This number includes deleted (hidden) records
     @return number of records in the DBF file.
     */
    public int getRecordCount() {
        return this.header.numberOfRecords;
    }

    /**
     * Returns objects in the designated record. if record index out of bound, it returns DBFException
     *
     * @param recordIndex Index of the record. Record index starts from 0
     * @return object array for selected record
     */
    public Object[] getRecordOjects(int recordIndex) {
        List<Object> recordObjects = new ArrayList<>(this.getFieldCount());
        byte[] byteRecord = this.readRecord(recordIndex);
        boolean isDeleted = byteRecord[0] == '*';
        if (isDeleted && !showDeletedRows) {
            return null;
        }
        //skip heading byte
        int fieldOffset = 1;

        for (int i = 0; i < this.header.fieldArray.length; i++) {
            DBFField field = this.header.fieldArray[i];
            Object o = getFieldValue(field, byteRecord, fieldOffset);
            fieldOffset += field.getLength();
            if (field.isSystem()) {
                if (field.getType() == DBFDataType.NULL_FLAGS && o instanceof BitSet) {
                    BitSet nullFlags = (BitSet) o;
                    int currentIndex = -1;
                    for (int j = 0; j < this.header.fieldArray.length; j++) {
                        DBFField field1 = this.header.fieldArray[j];
                        if (field1.isNullable()) {
                            currentIndex++;
                            if (nullFlags.get(currentIndex)) {
                                recordObjects.set(j, null);
                            }
                        }
                        if (field1.getType() == DBFDataType.VARBINARY || field1.getType() == DBFDataType.VARCHAR) {
                            currentIndex++;
                            if (recordObjects.get(i) instanceof byte[]) {
                                byte[] data = (byte[]) recordObjects.get(j);
                                int size = field1.getLength();
                                if (!nullFlags.get(currentIndex)) {
                                    // Data is not full
                                    size = data[data.length - 1];
                                }
                                byte[] newData = new byte[size];
                                System.arraycopy(data, 0, newData, 0, size);
                                Object o1 = newData;
                                if (field1.getType() == DBFDataType.VARCHAR) {
                                    o1 = new String(newData, getCharset());
                                }
                                recordObjects.set(j, o1);
                            }
                        }
                    }
                }
            } else {
                recordObjects.add(o);
            }
        }
        return recordObjects.toArray();
    }

    /**
     * Returns designated object in one record. if index out of bound, it returns DBFException
     *
     * @param recordIndex Index of the record. Record index starts from 0
     * @param fieldIndex Index of the field. Field index starts from 0
     * @return object array for selected record
     */
    public Object getRecordObjects(int recordIndex, int fieldIndex) {
        Object[] record = getRecordOjects(recordIndex);
        if (record == null) {
            return null;
        }
        if (!(fieldIndex >= 0 && fieldIndex < this.getFieldCount())) {
            throw new DBFException("Invalid field position " + String.valueOf(fieldIndex));
        }
        return record[fieldIndex];
    }

    /**
     * Returns designated DBFRow, otherwise return null
     *
     * @param recordIndex Index of the record. Record index starts from 0
     * @return object array for selected record
     */
    public DBFRow getRecord(int recordIndex) {
        Object[] record = getRecordOjects(recordIndex);
        if (record == null) {
            return null;
        }
        return new DBFRow(record, mapFieldNames, this.header.fieldArray);
    }

    /**
     * update a record
     *
     * @param recordIndex Index of the field. Record index starts from 0.
     * @param objectArray field array
     * @throws IOException write DBF exception
     */
    public void updateRecord(int recordIndex, Object[] objectArray) throws IOException {
        this.raf.write((byte) ' ');
        for (int i = 0; i < this.header.fieldArray.length; i++) {
            /* iterate through all fields */
            this.updateRecord(recordIndex,i, objectArray[i]);
        }
    }

    /**
     * update a field in one record
     *
     * @param recordIndex Index of the field. Record index starts from 0.
     * @param fieldName name of the field.
     * @param obj input object
     * @throws IOException write DBF exception
     */
    public void updateRecord(int recordIndex, String fieldName, Object obj) throws IOException {
        for (int i = 0; i < this.header.fieldArray.length ; i ++) {
            if (fieldName.equals(this.header.fieldArray[i].getName())) {
                this.updateRecord(recordIndex, i, obj);
                break;
            }
        }
    }

    /**
     * update a field in one record
     *
     * @param recordIndex Index of the field. Record index starts from 0.
     * @param fieldIndex Index of the field. Record index starts from 0.
     * @param obj input object
     * @throws IOException write DBF exception
     */
    public void updateRecord(int recordIndex, int fieldIndex, Object obj) throws IOException {
        if (this.closed) {
            throw new IllegalStateException("DBFRandomAccess has been closed");
        }
        if (this.header.fieldArray == null) {
            throw new DBFException("Invalid dbf header");
        }
        if (obj == null) {
            throw new DBFException("Null field");
        }

        if (!(recordIndex >= 0 && recordIndex < this.recordCount)) {
            throw new DBFException("Invalid record position " + String.valueOf(recordIndex));
        }
        if (!(fieldIndex >= 0 && fieldIndex < this.getFieldCount())) {
            throw new DBFException("Invalid field position " + String.valueOf(fieldIndex));
        }

        int nOffset = this.header.headerLength + this.header.recordLength * recordIndex + 1;
        for (int j = 0; j < this.header.fieldArray.length; j++) {
            if (fieldIndex == j) {
                break;
            }
            else {
                nOffset += this.header.fieldArray[j].getLength();
            }
        }
        writeToDBF(nOffset, this.header.fieldArray[fieldIndex], obj);

    }
    /**
     * Add a record.
     * @param values fields of the record
     */
    public void addRecord(Object[] values) {

        if (this.closed) {
            throw new IllegalStateException("You can add records a closed DBFRandomAccess");
        }
        if (this.header.fieldArray == null) {
            throw new DBFException("Fields should be set before adding records");
        }

        if (values == null) {
            throw new DBFException("Null cannot be added as row");
        }

        if (values.length != this.header.fieldArray.length) {
            throw new DBFException("Invalid record. Invalid number of fields in row");
        }

        int nOffset = this.header.headerLength + this.header.recordLength * this.recordCount;
        for (int i = 0; i < this.header.fieldArray.length; i++) {
            try {
                writeToDBF(nOffset, this.header.fieldArray[i], values[i]);
                nOffset += this.header.fieldArray[i].getLength();
            } catch (IOException e) {
                throw new DBFException("Error occured while writing record. " + e.getMessage(), e);
            }
        }
        this.recordCount++;
    }

    private Object getFieldValue(DBFField field, byte[] byteRecords, int offset) {
        switch (field.getType()) {
            case CHARACTER:
                byte b_array[] = subBytes(byteRecords, offset, field.getLength());
                if (this.trimRightSpaces) {
                    return new String(DBFUtils.trimRightSpaces(b_array), getCharset());
                }
                else {
                    return new String(b_array, getCharset());
                }

            case VARCHAR:
            case VARBINARY:
                byte b_array_var[] = subBytes(byteRecords, offset, field.getLength());
                return b_array_var;
            case DATE:

                byte t_byte_year[] = subBytes(byteRecords, offset, 4);
                byte t_byte_month[] = subBytes(byteRecords, offset + 4 , 2);
                byte t_byte_day[] = subBytes(byteRecords, offset + 6, 2);

                try {
                    GregorianCalendar calendar = new GregorianCalendar(Integer.parseInt(new String(t_byte_year,
                            StandardCharsets.US_ASCII)),
                            Integer.parseInt(new String(t_byte_month, StandardCharsets.US_ASCII)) - 1,
                            Integer.parseInt(new String(t_byte_day, StandardCharsets.US_ASCII)));
                    return calendar.getTime();
                } catch (NumberFormatException e) {
                    // this field may be empty or may have improper value set
                    return null;
                }


            case FLOATING_POINT:
            case NUMERIC:
                return DBFUtils.toNumeric(subBytes(byteRecords, offset, field.getLength()));

            case LOGICAL:
                byte [] t_logical = subBytes(byteRecords, offset, field.getLength());
                return DBFUtils.toBoolean(t_logical[0]);
            case LONG:
            case AUTOINCREMENT:
                byte [] b_array_long = subBytes(byteRecords, offset, field.getLength());
                int data = DBFUtils.toLittleEndianInt(b_array_long);
                return data;
            case CURRENCY:
                byte [] b_array_cny = subBytes(byteRecords, offset, 4);
                int c_data = DBFUtils.toLittleEndianInt(b_array_cny);
                String s_data = String.format("%05d", c_data);
                String x1 = s_data.substring(0, s_data.length() - 4);
                String x2 = s_data.substring(s_data.length() - 4);

                return new BigDecimal(x1 + "." + x2);
            case TIMESTAMP:
            case TIMESTAMP_DBASE7:
                int days = DBFUtils.toLittleEndianInt(subBytes(byteRecords, offset, 4));
                int time = DBFUtils.toLittleEndianInt(subBytes(byteRecords, offset + 4, 4));

                if(days == 0 && time == 0) {
                    return null;
                }
                else {
                    Calendar calendar = new GregorianCalendar();
                    calendar.setTimeInMillis(days * MILLISECS_PER_DAY + TIME_MILLIS_1_1_4713_BC + time);
                    calendar.add(Calendar.MILLISECOND, -TimeZone.getDefault().getOffset(calendar.getTimeInMillis()));
                    return calendar.getTime();
                }
            case MEMO:
            case GENERAL_OLE:
            case PICTURE:
            case BLOB:
                Number nBlock =  null;
                if (field.getLength() == 10) {
                    nBlock = DBFUtils.toNumeric(subBytes(byteRecords, offset, field.getLength()));
                }
                else {
                    nBlock = DBFUtils.toLittleEndianInt(subBytes(byteRecords, offset, 4));
                }
                if (this.memoFile != null && nBlock != null) {
                    return memoFile.readData(nBlock.intValue(), field.getType());
                }
                return null;
            case BINARY:
                if (field.getLength() == 8) {
                    return  DBFUtils.toDouble(subBytes(byteRecords, offset, 8));
                }
                else {
                    nBlock =  null;
                    if (field.getLength() == 10) {
                        nBlock = DBFUtils.toNumeric(subBytes(byteRecords, offset, field.getLength()));
                    }
                    else {
                        nBlock = DBFUtils.toLittleEndianInt(subBytes(byteRecords, offset, 4));
                    }
                    if (this.memoFile != null && nBlock != null) {
                        return memoFile.readData(nBlock.intValue(), field.getType());
                    }
                    return null;
                }
            case DOUBLE:
                return DBFUtils.toDouble(subBytes(byteRecords, offset, 8));
            case NULL_FLAGS:
                byte [] data1 = subBytes(byteRecords, offset, field.getLength());
                return BitSet.valueOf(data1);
            default:
                return null;
        }
    }

    private byte[] readRecord(int recordIndex) throws DBFException{
        if (this.closed) {
            throw new DBFException("DBFRandomAccess is closed");
        }
        byte[] byteRecord = null;
        int len = 0;
        try {
            if (recordIndex >= 0 && recordIndex < this.recordCount) {
                this.raf.seek(this.header.headerLength + this.header.recordLength * recordIndex);
            } else {
                throw new DBFException("Invalid record position " + String.valueOf(recordIndex));
            }

            byteRecord = new byte[this.header.recordLength];
            len = this.raf.read(byteRecord);
        } catch (IOException e)
        {
            throw new DBFException(e.getMessage(),e);
        }
        if (len != this.header.recordLength) {
            throw new DBFException("Record length exception: " + String.valueOf(len) + " VS " +
                    String.valueOf(this.header.recordLength));
        }
        return byteRecord;
    }


    private Map<String, Integer> createMapFieldNames(DBFField[] fieldArray) {
        Map<String, Integer> fieldNames = new HashMap<String, Integer>();
        for (int i = 0; i < fieldArray.length; i++) {
            String name = fieldArray[i].getName();
            fieldNames.put(name.toLowerCase(), i);
        }
        return Collections.unmodifiableMap(fieldNames);
    }




    private byte[] subBytes(byte[] src, int begin, int count) {
        byte[] bs = new byte[count];
        System.arraycopy(src, begin, bs, 0, count);
        return bs;
    }

    private void writeToDBF(int nOffset,DBFField field, Object obj) throws IOException{
        this.raf.seek(nOffset);
        switch (field.getType()) {
            case CHARACTER:
                String strValue = "";
                if (obj != null) {
                    strValue = obj.toString();
                }
                this.raf.write(DBFUtils.textPadding(strValue, getCharset(),
                        field.getLength(), DBFAlignment.LEFT, (byte) ' '));

                break;

            case DATE:
                if (obj != null) {
                    GregorianCalendar calendar = new GregorianCalendar();
                    calendar.setTime((Date) obj);
                    this.raf.write(String.valueOf(calendar.get(Calendar.YEAR)).getBytes(StandardCharsets.US_ASCII));
                    this.raf.write(DBFUtils.textPadding(String.valueOf(calendar.get(Calendar.MONTH) + 1),
                            StandardCharsets.US_ASCII, 2, DBFAlignment.RIGHT, (byte) '0'));
                    this.raf.write(DBFUtils.textPadding(String.valueOf(calendar.get(Calendar.DAY_OF_MONTH)),
                            StandardCharsets.US_ASCII, 2, DBFAlignment.RIGHT, (byte) '0'));
                } else {
                    this.raf.write("        ".getBytes(StandardCharsets.US_ASCII));
                }

                break;
            case NUMERIC:
            case FLOATING_POINT:

                if (obj != null) {
                    this.raf.write(DBFUtils.doubleFormating((Number) obj, getCharset(),
                            field.getLength(), field.getDecimalCount()));
                } else {
                    this.raf.write(DBFUtils.textPadding(" ", getCharset(),
                            field.getLength(), DBFAlignment.RIGHT, (byte) ' '));
                }

                break;

            case LOGICAL:

                if (obj instanceof Boolean) {
                    if ((Boolean) obj) {
                        this.raf.write((byte) 'T');
                    } else {
                        this.raf.write((byte) 'F');
                    }
                } else {
                    this.raf.write((byte) '?');
                }

                break;

            default:
                throw new DBFException("Unknown field type " + field.getType());
        }
    }

    /**
     * Sets the memo file (DBT or FPT) where memo fields will be readed.
     * If no file is provided, then this fields will be null.
     * @param memoFile the file containing the memo data
     */
    public void setMemoFile(File memoFile) {
        if (this.memoFile != null) {
            throw new IllegalStateException("Memo file is already setted");
        }
        if (!memoFile.exists()){
            throw new DBFException("Memo file " + memoFile.getName() + " not exists");
        }
        if (!memoFile.canRead()) {
            throw new DBFException("Cannot read Memo file " + memoFile.getName());
        }
        this.memoFile = new DBFMemoFile(memoFile, this.getCharset());
    }

    @Override
    public Charset getCharset() {
        return this.header.getUsedCharset();
    }

    @Override
    public void close() throws DBFException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (this.raf != null) {
            /*
             * everything is written already. just update the header for
             * record count and the END_OF_DATA mark
             */
            try {
                this.header.numberOfRecords = this.recordCount;
                this.raf.seek(0);
                this.header.write(this.raf);
                this.raf.seek(this.raf.length());
                this.raf.writeByte(END_OF_DATA);
            }
            catch (IOException e) {
                throw new DBFException(e.getMessage(), e);
            }
            finally {
                DBFUtils.close(this.raf);
            }
        }
    }
}
