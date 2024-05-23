from pyshacl import validate
import sys
from rdflib import Graph

def validate_using_shacl_mineral_site(data_graph):

    resources = """
    
    mndr:Measured a mndr:ResourceReserveCategory .
    
    mndr:Indicated a mndr:ResourceReserveCategory .
    
    mndr:Inferred a mndr:ResourceReserveCategory .
    
    mndr:Probable a mndr:ResourceReserveCategory .
    
    mndr:Proven a mndr:ResourceReserveCategory .
    
    mndr:Extracted a mndr:ResourceReserveCategory .
    
    mndr:OriginalResource a mndr:ResourceReserveCategory .
    
    mndr:CumulativeExtracted a mndr:ResourceReserveCategory .
    
    """

    data_graph = data_graph + resources

    shapes_graph = """
    @prefix gkbp:  <https://geokb.wikibase.cloud/wiki/Property:> .
    @prefix owl:   <http://www.w3.org/2002/07/owl#> .
    @prefix dcam:  <http://purl.org/dc/dcam/> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix skos:  <http://www.w3.org/2004/02/skos/core#> .
    @prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix geo:   <http://www.opengis.net/ont/geosparql#> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix sh:    <http://www.w3.org/ns/shacl#> .
    @prefix xml:   <http://www.w3.org/XML/1998/namespace> .
    @prefix dcterms: <http://purl.org/dc/terms/> .
    @prefix gkbi:  <https://geokb.wikibase.cloud/entity/> .
    @prefix mndr:  <https://minmod.isi.edu/resource/> .
    @prefix prov:  <http://www.w3.org/ns/prov#> .
    @prefix ex:  <http://www.w3.org/ns/prov#> .
      
    mndr:Document  a     sh:NodeShape  ;
    sh:targetClass mndr:Document;
    sh:class mndr:Document;
        sh:property [   
            sh:path mndr:issue ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:doi ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:description ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:journal ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:year ;
            sh:minCount 0 ;
            sh:datatype xsd:integer ;
        ];
        sh:property [   
            sh:path mndr:month ;
            sh:minCount 0 ;
            sh:datatype xsd:integer ;
        ];
        sh:property [   
            sh:path mndr:volume ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:authors ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:title ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ] ;
        sh:property [   
            sh:path mndr:uri ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ] ;
        sh:property [   
            sh:path mndr:id ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        .
                          
    mndr:Reference  a  sh:NodeShape ;
    sh:targetClass mndr:Reference;
    sh:class mndr:Reference;
        sh:property [   
            sh:path mndr:document ;
            sh:minCount 0 ;
            sh:class mndr:Document ;
        ];
        sh:property [   
            sh:path mndr:page_info ;
            sh:minCount 0 ;
            sh:class mndr:PageInfo ;
        ];
        .
        
    mndr:MineralInventory  a  sh:NodeShape ;
    sh:targetClass mndr:MineralInventory;
    sh:class mndr:MineralInventory;
            sh:property [   
            sh:path mndr:grade ;
            sh:minCount 0 ;
            sh:class mndr:Grade ;
        ];
        sh:property [   
            sh:path mndr:ore ;
            sh:minCount 0 ;
            sh:class mndr:Ore ;
        ];
        sh:property [   
            sh:path mndr:cutoff_grade ;
            sh:minCount 0 ;
            sh:class mndr:Grade ;
        ];
        sh:property [   
            sh:path mndr:reference ;
            sh:minCount 0 ;
            sh:class mndr:Reference ;
        ];
        sh:property [   
            sh:path mndr:category ;
            sh:minCount 0 ;
        ] ;
        sh:property [   
            sh:path mndr:commodity ;
            sh:minCount 0 ;
            sh:nodeKind sh:IRI ;
        ] ;
        sh:property [   
            sh:path mndr:observed_commodity ;
            sh:minCount 0 ;
        ] ;
        sh:property [   
            sh:path mndr:date ;
            sh:minCount 0 ;
            sh:datatype xsd:date ;
        ] ;
        sh:property [   
            sh:path mndr:id ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        .
        
    mndr:MineralSite  a  sh:NodeShape ;
    sh:targetClass mndr:MineralSite;
    sh:class mndr:MineralSite;
            sh:property [   
            sh:path mndr:deposit_type_candidate ;
            sh:or ( [ sh:nodeKind sh:IRI ; ] [ sh:class mndr:DepositTypeCandidate ] ) ;
        ];
        sh:property [   
            sh:path mndr:location_info ;
            sh:minCount 0 ;
            sh:class mndr:LocationInfo ;
        ];
        sh:property [   
            sh:path mndr:geology_info ;
            sh:minCount 0 ;
            sh:class mndr:GeologyInfo ;
        ];
        sh:property [   
            sh:path mndr:mineral_inventory ;
            sh:minCount 0 ;
            sh:class mndr:MineralInventory ;
        ];
         sh:property [   
            sh:path mndr:name ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ] ;
        sh:property [   
            sh:path mndr:id ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:record_id ;
            sh:minCount 0 ;
            sh:or ( [ sh:datatype xsd:string ] [ sh:datatype xsd:integer ] ) ;
        ];
        sh:property [   
            sh:path mndr:source_id ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        .
        
    
    mndr:Ore  a          sh:NodeShape;
    sh:targetClass mndr:Ore;
    sh:class  mndr:Ore ;
           sh:property [
                            sh:path mndr:ore_unit ;
                            sh:nodeKind sh:IRI ;
                        ] ;
            sh:property [
                            sh:path mndr:ore_value ;
                            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] ) ;
                        ] .
     
     mndr:Grade  a          sh:NodeShape;
    sh:targetClass mndr:Grade;
    sh:class  mndr:Grade ;
           sh:property [
                            sh:path mndr:grade_unit ;
                            sh:nodeKind sh:IRI ; ;
                        ] ;
            sh:property [
                            sh:path mndr:grade_value ;
                            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] [ sh:datatype xsd:double ]) ;
                        ] .
    
                                        
    mndr:DepositTypeCandidate  a          sh:NodeShape;
    sh:targetClass mndr:DepositTypeCandidate;
           sh:property [
                            sh:path mndr:normalized_uri ;
                            sh:nodeKind sh:IRI ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:id ;
                            sh:nodeKind sh:IRI ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:observed_name ;
                        ] ;
                        sh:property [
                            sh:path mndr:source ;
                        ] ;
                        sh:property [
                            sh:path mndr:confidence ;
                            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] [ sh:datatype xsd:double ]) ;
                        ]
                        .
                        
    mndr:LocationInfo  a          sh:NodeShape;
    sh:targetClass mndr:LocationInfo;
    sh:class mndr:LocationInfo;
           sh:property [
                            sh:path mndr:country ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ];
              sh:property [
                            sh:path mndr:location ;
                            sh:datatype  geo:wktLiteral ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ];
                        sh:property [
                            sh:path mndr:state_or_province ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:location_source_record_id ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:location_source ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                         sh:property [
                            sh:path mndr:crs ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ].
                        
    
                
    mndr:GeologyInfo  a          sh:NodeShape;
    sh:targetClass mndr:GeologyInfo;
    sh:class mndr:GeologyInfo;
           sh:property [
                            sh:path mndr:unit_name ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ];
              sh:property [
                            sh:path mndr:lithology ;
                            sh:datatype  xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ];
                        sh:property [
                            sh:path mndr:process ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:environment ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:age ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                        sh:property [
                            sh:path mndr:description ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ] ;
                         sh:property [
                            sh:path mndr:comments ;
                            sh:datatype xsd:string ;
                            sh:minCount 0 ;  
                            sh:maxCount 1 ;
                        ].
                        
    
    
    mndr:PageInfo a   sh:NodeShape ;
    sh:class  mndr:PageInfo ;
    sh:targetClass mndr:PageInfo;
            sh:property [
                            sh:path mndr:page ;
                            sh:datatype xsd:integer ;
                        ];
            sh:property [
                            sh:path mndr:bounding_box ;
                            sh:class mndr:BoundingBox ;
                        ].
    
    
    """
    shapes_g = Graph().parse(data=shapes_graph, format="turtle")

    result = validate(data_graph, shacl_graph=shapes_g, inference='rdfs', serialize_report_graph=True)

    conforms, a, b = result

    if not conforms:
        print("Validation does not conform. There are violations.")
        print(b)
        return False
    else:
        print('This is fine')

    return True




def validate_mineral_system_using_shacl(data_graph):


    data_graph = data_graph

    shapes_graph = """
    @prefix gkbp:  <https://geokb.wikibase.cloud/wiki/Property:> .
    @prefix owl:   <http://www.w3.org/2002/07/owl#> .
    @prefix dcam:  <http://purl.org/dc/dcam/> .
    @prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
    @prefix skos:  <http://www.w3.org/2004/02/skos/core#> .
    @prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix geo:   <http://www.opengis.net/ont/geosparql#> .
    @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix sh:    <http://www.w3.org/ns/shacl#> .
    @prefix xml:   <http://www.w3.org/XML/1998/namespace> .
    @prefix dcterms: <http://purl.org/dc/terms/> .
    @prefix gkbi:  <https://geokb.wikibase.cloud/entity/> .
    @prefix mndr:  <https://minmod.isi.edu/resource/> .
    @prefix prov:  <http://www.w3.org/ns/prov#> .
    @prefix ex:  <http://www.w3.org/ns/prov#> .
    
    mndr:MappableCriteria-https___minmod.isi.edu_resource_criteria
            a        sh:PropertyShape ;
            sh:path  mndr:criteria .
      
    mndr:Document  a     sh:NodeShape  ;
    sh:targetClass mndr:Document;
    sh:class mndr:Document;
        sh:property [   
            sh:path mndr:issue ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:doi ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:description ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:journal ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:year ;
            sh:minCount 0 ;
            sh:datatype xsd:integer ;
        ];
        sh:property [   
            sh:path mndr:month ;
            sh:minCount 0 ;
            sh:datatype xsd:integer ;
        ];
        sh:property [   
            sh:path mndr:volume ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:authors ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        sh:property [   
            sh:path mndr:title ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ] ;
        sh:property [   
            sh:path mndr:uri ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ] ;
        sh:property [   
            sh:path mndr:id ;
            sh:minCount 0 ;
            sh:datatype xsd:string ;
        ];
        .
        
    mndr:BoundingBox  a  sh:NodeShape ;
    sh:targetClass mndr:BoundingBox;
    sh:class mndr:BoundingBox;
        sh:property [   
            sh:path mndr:x_min ;
            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] [sh:datatype xsd:string] ) ;
        ];
        sh:property [   
            sh:path mndr:x_max ;
            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] [sh:datatype xsd:string]) ;
        ];
        sh:property [   
            sh:path mndr:y_min ;
            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] [sh:datatype xsd:string]) ;
        ];
        sh:property [   
            sh:path mndr:y_max ;
            sh:or ( [ sh:datatype xsd:decimal ] [ sh:datatype xsd:integer ] [sh:datatype xsd:string]) ;
        ];
        .
                          
    mndr:Reference  a  sh:NodeShape ;
    sh:targetClass mndr:Reference;
    sh:class mndr:Reference;
        sh:property [   
            sh:path mndr:document ;
            sh:minCount 0 ;
            sh:class mndr:Document ;
        ];
        sh:property [   
            sh:path mndr:page_info ;
            sh:minCount 0 ;
            sh:class mndr:PageInfo ;
        ];
        .
        
    mndr:MappableCriteria  a  sh:NodeShape ;
    sh:targetClass mndr:MappableCriteria;
    sh:class mndr:MappableCriteria;
        sh:property [   
            sh:path mndr:potential_dataset ;
            sh:minCount 0 ;
            sh:class mndr:EvidenceLayer ;
        ];
        sh:property [   
            sh:path mndr:supporting_reference ;
            sh:minCount 0 ;
            sh:class mndr:Reference ;
        ];
        sh:property [   
            sh:path mndr:criteria ;
            sh:minCount 0 ;
        ] ;
        sh:property [   
            sh:path mndr:theorectical ;
            sh:minCount 0 ;
        ] 
        .
        
    mndr:MineralSystem  a  sh:NodeShape ;
    sh:targetClass mndr:MineralSystem;
    sh:class mndr:MineralSystem;
            sh:property [   
            sh:path mndr:deposit_type ;
            sh:or ( [ sh:nodeKind sh:IRI ; ] [ sh:class mndr:DepositType ] ) ;
        ];
        sh:property [   
            sh:path mndr:source ;
            sh:minCount 1 ;
            sh:class mndr:MappableCriteria ;
        ];
        sh:property [   
            sh:path mndr:pathway ;
            sh:minCount 1 ;
            sh:class mndr:MappableCriteria ;
        ];
        sh:property [   
            sh:path mndr:trap ;
            sh:minCount 0 ;
            sh:class mndr:MappableCriteria ;
        ];
        sh:property [   
            sh:path mndr:preservation ;
            sh:minCount 0 ;
            sh:class mndr:MappableCriteria ;
        ];
        sh:property [   
            sh:path mndr:outflow ;
            sh:minCount 0 ;
            sh:class mndr:MappableCriteria ;
        ];
        sh:property [   
            sh:path mndr:energy ;
            sh:minCount 0 ;
            sh:class mndr:MappableCriteria ;
        ];
        .

                                        
    mndr:DepositType  a          sh:NodeShape;
    sh:targetClass mndr:DepositType;
                        sh:property [
                            sh:path mndr:name ;
                        ] ;
                        sh:property [
                            sh:path mndr:environment ;
                        ] ;
                        sh:property [
                            sh:path mndr:group ;
                        ]
                        .
                        
    
    mndr:PageInfo a   sh:NodeShape ;
    sh:class  mndr:PageInfo ;
    sh:targetClass mndr:PageInfo;
            sh:property [
                            sh:path mndr:page ;
                            sh:datatype xsd:integer ;
                        ];
            sh:property [
                            sh:path mndr:bounding_box ;
                            sh:class mndr:BoundingBox ;
                        ].
    
    
    """
    shapes_g = Graph().parse(data=shapes_graph, format="turtle")

    result = validate(data_graph, shacl_graph=shapes_g, inference='rdfs', serialize_report_graph=True)

    conforms, a, b = result

    if not conforms:
        print("Validation does not conform. There are violations.")
        print(b)
        return False
    else:
        print('This is fine')

    return True