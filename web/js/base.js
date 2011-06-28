var markerlist = [];
var image_size = 120;
var currentmax = 0;
var currentmin = 10000000000;
var years = ['1584', '1590', '1592', '1596', '1600', '1610', '1620', '1628', '1635', '1647', '1648', '1649', '1656', '1658', '1660', '1661', '1662', '1663', '1664', '1665', '1666', '1667', '1668', '1669', '1670', '1676', '1680', '1681', '1683', '1684', '1690', '1691', '1692', '1693', '1694', '1695', '1697', '1698', '1699', '1701', '1703', '1706', '1708', '1709', '1711', '1713', '1714', '1717', '1719', '1722', '1723', '1724', '1725', '1727', '1728', '1729', '1730', '1731', '1732', '1734', '1735', '1736', '1737', '1738', '1741', '1743', '1745', '1747', '1749', '1750', '1751', '1752', '1753', '1754', '1755', '1757', '1758', '1759', '1761', '1762', '1763', '1764', '1765', '1766', '1767', '1768', '1769', '1770', '1771', '1772', '1773', '1774', '1775', '1776', '1777', '1778', '1779', '1780', '1782', '1783', '1784', '1785', '1786', '1787', '1788', '1789', '1790', '1791', '1792', '1793', '1794', '1795', '1796', '1797', '1798', '1799', '1800', '1801', '1802', '1803', '1804', '1805', '1806', '1807', '1808', '1809', '1810', '1811', '1812', '1813', '1814', '1815', '1816', '1817', '1818', '1819', '1820', '1821', '1822', '1823', '1824', '1825', '1826', '1827', '1828', '1829', '1830', '1831', '1832', '1833', '1834', '1835', '1836', '1837', '1838', '1839', '1840', '1841', '1842', '1843', '1844', '1845', '1846', '1847', '1848', '1849', '1850', '1851', '1852', '1853', '1854', '1855', '1856', '1857', '1858', '1859', '1860', '1861', '1862', '1863', '1864', '1865', '1866', '1867', '1868', '1869', '1870', '1871', '1872', '1873', '1874', '1875', '1876', '1877', '1878', '1879', '1880', '1881', '1882', '1883', '1884', '1885', '1886', '1887', '1888', '1889', '1890', '1891', '1892', '1893', '1894', '1895', '1896', '1897', '1898', '1899', '1900', '1901', '1902', '1903', '1904', '1905', '1906', '1907', '1908', '1909', '1910', '1911', '1912', '1913', '1914', '1915', '1916', '1917', '1918', '1919', '1920', '1921', '1922', '1923', '1924', '1925', '1926', '1927', '1928', '1929', '1930', '1931', '1932', '1933', '1934', '1935', '1936', '1937', '1938', '1939', '1940', '1941', '1942', '1943', '1944', '1945', '1946', '1947', '1948', '1949', '1950', '1951', '1952', '1953', '1954', '1955', '1956', '1957', '1958', '1959', '1960', '1961', '1962', '1963', '1964', '1965', '1966', '1967', '1968', '1969', '1970', '1971', '1972', '1973', '1974', '1975', '1976', '1977', '1978', '1979', '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009'];
var my_map = null;
var initlatlon = new google.maps.LatLng(39.055824, -95.689018); //Topeka, Kansas

//parseData clears the Map: count, city, long, year, lat, long

function parseData(data) {
	for (var i=0; i<markerlist.length; i++) {
		markerlist[i].setMap(null);
	}
	markerlist = []; //make sure its clear
	numcounts = []; //make sure its clear
	currentmax = 0;
	currentmin = 10000000000;
	for (var i=0; i<data.length; i++) {
		if (data[i].count > currentmax) currentmax = data[i].count;
		if (data[i].count < currentmin) currentmin = data[i].count;
	}
	var testmax = data.length;
	if (testmax > 10) testmax = 10;
	for (var i=0; i<testmax; i++) {
		var test_url = "http://chart.apis.google.com/chart?chst=d_bubble_texts_big&chld=edge_tl|000000|ffffff| "+data[i].count;
		var shadowimage = new google.maps.MarkerImage(test_url);

		// given the value use chart api to get a colored marker
		var my_point = new google.maps.LatLng(data[i].lat, data[i].long);
		var marker = new google.maps.Marker({
			shadow: makeIcon(data[i].count, currentmin, currentmax),
			position: my_point,
			map: null,
			icon: shadowimage,
			metadata: [data[i].city, data[i].count]
		});
		//google.maps.event.addListener(marker, 'click', function() {
			//dispinfo(marker);
		//});
		markerlist.push(marker);
	}
	drawMarkers();
	return false;
}

//returns a chart api URL
function makeIcon(val, minval, maxval) {
	//pick color based on
	var scaledvalue = val/(maxval-minval);
	var color = "00"+parseInt(Math.floor(255*scaledvalue)).toString(16)+"00";
	var localimage_size = Math.floor(image_size*scaledvalue);
	iconurl = "http://mapscripting.com/examples/overlays/circle.png";
	var image = new google.maps.MarkerImage(iconurl,
		            new google.maps.Size(localimage_size, localimage_size),
		    	    new google.maps.Point(0, 0),
		    	    new google.maps.Point(localimage_size/2, localimage_size/2),
		            new google.maps.Size(localimage_size, localimage_size)
			    );
	return image;
}

function makeBasicQuery(yearindex) {
	//var srcstring = "http://pixels.media.mit.edu/hadoop/test/JSON.real."+years[yearindex];
	var srcstring = "data/JSON.real."+years[yearindex];
	injectScript(srcstring);
	return false;
}

function dispinfo(marker) {
	var val = marker.metadata;
	var contentString = '<div id="content">'+
    	'<h1>'+val[0]+', frequency count: '+val[1]+
    	'</h1></div>';

	var infowindow = new google.maps.InfoWindow({
    		content: contentString
	});
			
	infowindow.open(my_map,marker);
}

function injectScript(srcstring) {
	var headID = document.getElementsByTagName("head")[0];
	var newScript = document.createElement('script');
	newScript.type = 'text/javascript';
	newScript.src = srcstring
	headID.appendChild(newScript);
	return false;
}

function MapThing() {
	var myOptions = {
		zoom:3,
		center: initlatlon,
		mapTypeId: google.maps.MapTypeId.ROADMAP,
		streetViewControl: false,
		mapTypeControl: false,
	}
	
	my_map = new google.maps.Map(document.getElementById("map_canvas"), myOptions);
	my_map.setCenter(initlatlon);
	return false;
}

function drawMarkers() {
	for (var i=0; i<markerlist.length; i++) {
		markerlist[i].setMap(my_map);
	}
	return false;
}

// Main function calls
$(document).ready(function() {
	
	var initindex = Math.floor(years.length/2);	
	myMap = new MapThing();
	makeBasicQuery(initindex);
	var cap = $("#amount h1").empty();
	cap = $("<h1/>").text(years[initindex]).appendTo("#amount");

	// slider stuff
	//$( "#slider" ).slider();
	$( "#slider" ).slider({
			value: initindex,
			min: 0,
			max: years.length-1,
			step: 1,
			slide: function( event, ui ) {
				makeBasicQuery(ui.value);
				var cap = $("#amount h1").empty();
				cap = $("<h1/>").text(years[ui.value]).appendTo("#amount");
			}
		});
});
