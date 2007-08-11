/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.hypertable.Hypertable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;
import java.util.Vector;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Schema {

    static final Logger log = Logger.getLogger("org.hypertable.Hypertable");

    public Schema(String schemaStr) throws IOException, ParseException, ParserConfigurationException, SAXException {
	ByteArrayInputStream bais = new ByteArrayInputStream(schemaStr.getBytes());
	DocumentBuilderFactory dbfactory = DocumentBuilderFactory.newInstance();
	DocumentBuilder builder = dbfactory.newDocumentBuilder();
	Document doc = builder.parse(bais);
	int maxId = 0;

	NodeList schemaNodes = doc.getElementsByTagName("Schema");

	if (schemaNodes.getLength() != 1)
	    throw new ParseException("Invalid number of Schema elements " + schemaNodes.getLength(), 0);

	Node node = schemaNodes.item(0);

	NamedNodeMap nodeMap = node.getAttributes();
	
	if ((node = nodeMap.getNamedItem("generation")) == null)
	    throw new ParseException("No 'generation' attribute found in Schema element", 0);

	mGeneration = Integer.parseInt(node.getNodeValue());

	NodeList agNodes = doc.getElementsByTagName("AccessGroup");

	for (int i=0; i<agNodes.getLength(); i++) {
	    AccessGroup ag = new AccessGroup();
	    node = agNodes.item(i);
	    NodeList cfNodes =  node.getChildNodes();
	    nodeMap = node.getAttributes();
	    node = nodeMap.getNamedItem("name");
	    ag.name = node.getNodeValue();
	    if ((node = nodeMap.getNamedItem("inMemory")) != null) {
		if (node.getNodeValue().equals("true"))
		    ag.inMemory = true;
	    }
	    if ((node = nodeMap.getNamedItem("blockSize")) != null)
		ag.blockSize = Integer.parseInt(node.getNodeValue().trim());

	    for (int j=0; j<cfNodes.getLength(); j++) {
		Node cfNode = cfNodes.item(j);
		if (cfNode.getNodeType() == Node.ELEMENT_NODE) {
		    ColumnFamily cf = new ColumnFamily();
		    cf.ag = ag.name;
		    nodeMap = cfNode.getAttributes();
		    node = nodeMap.getNamedItem("id");
		    cf.id = Integer.parseInt(node.getNodeValue());
		    if (cf.id > maxId)
			maxId = cf.id;
		    NodeList childNodes = cfNode.getChildNodes();
		    for (int k=0; k<childNodes.getLength(); k++) {
			node = childNodes.item(k);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
			    if (node.getNodeName().equalsIgnoreCase("Name")) {
				cf.name = node.getTextContent().trim();
			    }
			    else if (node.getNodeName().equalsIgnoreCase("ExpireDays")) {
				cf.expireTime = (long)(Double.parseDouble(node.getTextContent().trim()) * 86400.0);
			    }
			    else if (node.getNodeName().equalsIgnoreCase("KeepCopies")) {
				cf.keepCopies = Integer.parseInt(node.getTextContent().trim());
			    }
			}
		    }
		    ag.columns.add(cf);
		    if (mColumnFamilyMap.containsKey(cf.name))
			throw new ParseException("Bad table schema, column family '" + cf.name + "' multiply defined", 0);
		    mColumnFamilyMap.put(cf.name, cf);
		}
	    }
	    mAccessGroups.add(ag);
	}

	/**
	 * Setup mColumnFamilyVector
	 */
	mColumnFamilyVector.setSize(maxId+1);
	for (AccessGroup ag : mAccessGroups) {
	    for (ColumnFamily cf : ag.columns) {
		mColumnFamilyVector.set(cf.id, cf);
	    }
	}
	
    }

    public int GetGeneration() { return mGeneration; }

    public ColumnFamily GetColumnFamily(String family) {
	return mColumnFamilyMap.get(family);
    }

    public ColumnFamily GetColumnFamily(int id) {
	return mColumnFamilyVector.get(id);
    }

    public LinkedList<AccessGroup> GetAccessGroups() { return mAccessGroups; }

    private int mGeneration;
    private HashMap<String, ColumnFamily>  mColumnFamilyMap = new HashMap<String, ColumnFamily>();
    private LinkedList<AccessGroup>        mAccessGroups = new LinkedList<AccessGroup>();
    private Vector<ColumnFamily>           mColumnFamilyVector = new Vector<ColumnFamily>();
}

