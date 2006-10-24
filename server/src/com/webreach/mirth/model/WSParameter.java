package com.webreach.mirth.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.wsif.schema.SchemaType;

import com.l2fprod.common.beans.BaseBeanInfo;

public class WSParameter {
	private String name;
	private String type;
	private String value = new String();
	private SchemaType schemaType;
	//Used for array types
	private String length;
	private boolean array;
	private boolean Null;
	private int minOccurs;
	private int maxOccurs;
	private boolean nillable = false;
	private boolean complex = false;
	private List<WSParameter> childParameters;
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public SchemaType getSchemaType() {
		return schemaType;
	}

	public void setSchemaType(SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	public String getLength() {
		return length;
	}

	public void setLength(String length) {
		this.length = length;
	}
	public String toString(){
		if (this.getSchemaType() != null)
			return this.getType() + " " + this.getName();
		else 
			return this.getType() + " " + this.getName() + " = " + this.getValue();
	}
	
	public boolean isArray() {
		return array;
	}

	public void setArray(boolean array) {
		this.array = array;
	}

	public boolean isNull() {
		return Null;
	}

	public void setNull(boolean null1) {
		Null = null1;
	}

	public List<WSParameter> getChildParameters() {
		if (childParameters == null){
			childParameters = new ArrayList<WSParameter>();
		}
		return childParameters;
	}

	public void getChildParameters(List<WSParameter> subParameters) {
		this.childParameters = subParameters;
	}

	public int getMaxOccurs() {
		return maxOccurs;
	}

	public void setMaxOccurs(int maxOccurs) {
		this.maxOccurs = maxOccurs;
		//we have an array
		if (maxOccurs == -1 || maxOccurs > 0){
			this.setArray(true);
		}
	}
	public void setMaxOccurs(String maxOccurs) {
		if (maxOccurs.equals("unbounded")){
			setMaxOccurs(-1); //TODO: Maybe use a better value?
		}else{
			try{
				setMaxOccurs(Integer.parseInt(maxOccurs));
			}catch(Exception e){}
		}
	}
	public int getMinOccurs() {
		return minOccurs;
	}

	public void setMinOccurs(int minOccurs) {
		this.minOccurs = minOccurs;
	}
	public void setMinOccurs(String minOccurs) {
		if (minOccurs.equals("unbounded")){
			setMinOccurs(-1); //TODO: Maybe use a better value?
		}else{
			try{
				setMinOccurs(Integer.parseInt(minOccurs));
			}catch(Exception e){}
		}
	}
	public boolean isNillable() {
		return nillable;
	}

	public void setNillable(boolean nillable) {
		this.nillable = nillable;
	}

	public boolean isComplex() {
		return complex;
	}

	public void setComplex(boolean complex) {
		this.complex = complex;
	}

}
