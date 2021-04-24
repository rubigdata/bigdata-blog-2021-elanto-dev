[< back to Assignment 4](assignment4.md)

```
%spark
object CT extends Serializable {
  
  import org.cts.CRSFactory;
  import org.cts.crs.GeodeticCRS;
  import org.cts.registry.EPSGRegistry;
  import org.cts.op.CoordinateOperationFactory;
  import org.cts.op.CoordinateOperation;

  // global variables to keep state for transformations
  @transient private var xy2latlonOp : CoordinateOperation = null;
  @transient private var latlon2xyOp : CoordinateOperation = null;

  // Create the coordinate transformation functions to convert from RD New to WGS:84 and vice versa
  def initTransforms : (CoordinateOperation, CoordinateOperation) = {
    // Create a new CRSFactory, a necessary element to create a CRS without defining one by one all its components
    val cRSFactory = new CRSFactory();

    // Add the appropriate registry to the CRSFactory's registry manager. Here the EPSG registry is used.
    val registryManager = cRSFactory.getRegistryManager();
    registryManager.addRegistry(new EPSGRegistry());

    // CTS will read the EPSG registry seeking the 4326 code, when it finds it,
    // it will create a CoordinateReferenceSystem using the parameters found in the registry.
    val crs1 : GeodeticCRS = (cRSFactory.getCRS("EPSG:28992")).asInstanceOf[GeodeticCRS];
    val crs2 : GeodeticCRS = (cRSFactory.getCRS("EPSG:4326") ).asInstanceOf[GeodeticCRS];
    
    // Transformation (x,y) -> (lon,lat)
    val xy2latlonOps = CoordinateOperationFactory.createCoordinateOperations(crs1,crs2);
    val xy2latlon = xy2latlonOps.iterator().next(); //get(0);
    
    val latlon2xyOps = CoordinateOperationFactory.createCoordinateOperations(crs2,crs1);
    val latlon2xy = latlon2xyOps.iterator().next(); //get(0);
    
    (xy2latlon, latlon2xy)
  }

  // Encapsulate private transient variable (for serializability of the object)
  def getXYOp : CoordinateOperation = {
    if (xy2latlonOp == null){
      val ts = initTransforms
      xy2latlonOp = ts._1
      latlon2xyOp = ts._2
    }
    xy2latlonOp
  }

  // Encapsulate private transient variable (for serializability of the object)
  def getLatLonOp : CoordinateOperation = {
    if (latlon2xyOp == null){
      val ts = initTransforms
      xy2latlonOp = ts._1
      latlon2xyOp = ts._2
    }
    latlon2xyOp
  }
  
  // Use the library's transformation function to convert the coordinates
  def transformXY(x:Float, y:Float) : (Float, Float) = {   
    // Note: easily confused, (lat,lon) <-> (y,x)
    val lonlat = this.getXYOp.transform(Array(x.toDouble, y.toDouble));
    return ( lonlat(1).toFloat, lonlat(0).toFloat)
  }
  
  // Use the library's transformation function to convert the coordinates
  def transformLatLon(lat:Float, lon:Float) : (Float, Float) = {
    // Note: easily confused, (lat,lon) <-> (y,x)
    val xy = this.getLatLonOp.transform(Array(lon.toDouble, lat.toDouble));
    return ( xy(0).toFloat, xy(1).toFloat)
  }
}

```