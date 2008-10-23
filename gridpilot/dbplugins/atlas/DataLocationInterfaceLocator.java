/**
 * DataLocationInterfaceLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package gridpilot.dbplugins.atlas;

public class DataLocationInterfaceLocator extends org.apache.axis.client.Service implements DataLocationInterface {

/**
 * gSOAP 2.6.0 generated service definition
 */

    public DataLocationInterfaceLocator() {
    }


    public DataLocationInterfaceLocator(org.apache.axis.EngineConfiguration config) {
        super(config);
    }

    public DataLocationInterfaceLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
        super(wsdlLoc, sName);
    }

    // Use to get a proxy class for DataLocationInterface
    //"https://lfc-atlas.cern.ch:8085/", "https://lxb1941.cern.ch:8085"
    // "http://lfc-atlas-test.cern.ch:8085"
    private java.lang.String DataLocationInterface_address = "http://lfc-atlas-test.cern.ch:8085";

    public java.lang.String getDataLocationInterfaceAddress() {
        return DataLocationInterface_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String DataLocationInterfaceWSDDServiceName = "DataLocationInterface";

    public java.lang.String getDataLocationInterfaceWSDDServiceName() {
        return DataLocationInterfaceWSDDServiceName;
    }

    public void setDataLocationInterfaceWSDDServiceName(java.lang.String name) {
        DataLocationInterfaceWSDDServiceName = name;
    }

    public DataLocationInterfacePortType getDataLocationInterface() throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
            endpoint = new java.net.URL(DataLocationInterface_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getDataLocationInterface(endpoint);
    }

    public DataLocationInterfacePortType getDataLocationInterface(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            DataLocationInterfaceStub _stub = new DataLocationInterfaceStub(portAddress, this);
            _stub.setPortName(getDataLocationInterfaceWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    public void setDataLocationInterfaceEndpointAddress(java.lang.String address) {
        DataLocationInterface_address = address;
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        try {
            if (DataLocationInterfacePortType.class.isAssignableFrom(serviceEndpointInterface)) {
                DataLocationInterfaceStub _stub = new DataLocationInterfaceStub(new java.net.URL(DataLocationInterface_address), this);
                _stub.setPortName(getDataLocationInterfaceWSDDServiceName());
                return _stub;
            }
        }
        catch (java.lang.Throwable t) {
            throw new javax.xml.rpc.ServiceException(t);
        }
        throw new javax.xml.rpc.ServiceException("There is no stub implementation for the interface:  " + (serviceEndpointInterface == null ? "null" : serviceEndpointInterface.getName()));
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(javax.xml.namespace.QName portName, Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        if (portName == null) {
            return getPort(serviceEndpointInterface);
        }
        java.lang.String inputPortName = portName.getLocalPart();
        if ("DataLocationInterface".equals(inputPortName)) {
            return getDataLocationInterface();
        }
        else  {
            java.rmi.Remote _stub = getPort(serviceEndpointInterface);
            ((org.apache.axis.client.Stub) _stub).setPortName(portName);
            return _stub;
        }
    }

    public javax.xml.namespace.QName getServiceName() {
        return new javax.xml.namespace.QName("http://cmsdoc.cern.ch/cms/grid/doc/DataLocationInterface.wsdl", "DataLocationInterface");
    }

    private java.util.HashSet ports = null;

    public java.util.Iterator getPorts() {
        if (ports == null) {
            ports = new java.util.HashSet();
            ports.add(new javax.xml.namespace.QName("http://cmsdoc.cern.ch/cms/grid/doc/DataLocationInterface.wsdl", "DataLocationInterface"));
        }
        return ports.iterator();
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        
if ("DataLocationInterface".equals(portName)) {
            setDataLocationInterfaceEndpointAddress(address);
        }
        else 
{ // Unknown Port Name
            throw new javax.xml.rpc.ServiceException(" Cannot set Endpoint Address for Unknown Port" + portName);
        }
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(javax.xml.namespace.QName portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        setEndpointAddress(portName.getLocalPart(), address);
    }

}
