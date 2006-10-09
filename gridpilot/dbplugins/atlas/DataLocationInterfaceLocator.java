/**
 * DataLocationInterfaceLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis WSDL2Java emitter.
 */

package gridpilot.dbplugins.atlas;

public class DataLocationInterfaceLocator extends
   org.apache.axis.client.Service implements DataLocationInterface {

  private static final long serialVersionUID=1L;
    // gSOAP 2.6.0 generated service definition

    //"https://lfc-atlas.cern.ch:8085/", "https://lxb1941.cern.ch:8085"
    // "http://lfc-atlas-test.cern.ch:8085"
    // Use to get a proxy class for DataLocationInterface
    private final java.lang.String DataLocationInterface_address =
       "http://lfc-atlas-test.cern.ch:8085";

    public java.lang.String getDataLocationInterfaceAddress() {
        return DataLocationInterface_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String DataLocationInterfaceWSDDServiceName =
       "DataLocationInterface";

    public java.lang.String getDataLocationInterfaceWSDDServiceName() {
        return DataLocationInterfaceWSDDServiceName;
    }

    public void setDataLocationInterfaceWSDDServiceName(java.lang.String name) {
        DataLocationInterfaceWSDDServiceName = name;
    }

    public DataLocationInterfacePortType getDataLocationInterface()
       throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
            endpoint = new java.net.URL(DataLocationInterface_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getDataLocationInterface(endpoint);
    }

    public DataLocationInterfacePortType getDataLocationInterface(
        java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            DataLocationInterfaceStub _stub = new DataLocationInterfaceStub(portAddress, this);
            _stub.setPortName(getDataLocationInterfaceWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(Class serviceEndpointInterface)
       throws javax.xml.rpc.ServiceException {
        try {
            if (DataLocationInterfacePortType.class.isAssignableFrom(
                serviceEndpointInterface)) {
                DataLocationInterfaceStub _stub =
                   new DataLocationInterfaceStub(new java.net.URL(
                   DataLocationInterface_address), this);
                _stub.setPortName(getDataLocationInterfaceWSDDServiceName());
                return _stub;
            }
        }
        catch (java.lang.Throwable t) {
            throw new javax.xml.rpc.ServiceException(t);
        }
        throw new javax.xml.rpc.ServiceException(
            "There is no stub implementation for the interface:  " +
            (serviceEndpointInterface == null ? "null" :
                                                serviceEndpointInterface.getName()));
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    public java.rmi.Remote getPort(javax.xml.namespace.QName portName,
       Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        if (portName == null) {
            return getPort(serviceEndpointInterface);
        }
        String inputPortName = portName.getLocalPart();
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
        return new javax.xml.namespace.QName(
            "http://cmsdoc.cern.ch/cms/grid/doc/DataLocationInterface.wsdl",
            "DataLocationInterface");
    }

    private java.util.HashSet ports = null;

    public java.util.Iterator getPorts() {
        if (ports == null) {
            ports = new java.util.HashSet();
            ports.add(new javax.xml.namespace.QName("DataLocationInterface"));
        }
        return ports.iterator();
    }

}
