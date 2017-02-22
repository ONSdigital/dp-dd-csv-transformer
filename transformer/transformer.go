package transformer

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/ONSdigital/dp-dd-csv-transformer/hierarchy"
	"github.com/ONSdigital/go-ns/log"
)

const (
	DIMENSION_START_INDEX  = 3
	HIERARCHY_ID_OFFSET    = 0
	DIMENSION_NAME_OFFSET  = 1
	DIMENSION_VALUE_OFFSET = 2
)

// CSVTransformer defines the CSVTransformer interface.
type CSVTransformer interface {
	Transform(r io.Reader, w io.Writer, hc hierarchy.HierarchyClient, requestId string) error
}

// Transformer implementation of the CSVTransformer interface.
type Transformer struct{}

// NewTransformer create a new Transformer.
func NewTransformer() *Transformer {
	return &Transformer{}
}

type Dimension struct {
	name           string
	dimensionIndex int
	columnIndex    int
	isHierarchical bool
	hierarchyType  string
	hc             hierarchy.HierarchyClient
}

// getDimensions parses the dimensions from an input csv row
func getDimensions(row []string, hc hierarchy.HierarchyClient) ([]*Dimension, error) {
	var result []*Dimension
	for i := DIMENSION_START_INDEX; i < len(row); i = i + 3 {
		var dim Dimension
		hierarchyId := strings.TrimSpace(row[i+HIERARCHY_ID_OFFSET])
		dim.isHierarchical = len(hierarchyId) > 0
		dim.name = strings.TrimSpace(row[i+DIMENSION_NAME_OFFSET])
		dim.columnIndex = i
		dim.dimensionIndex = len(result) + 1
		dim.hc = hc
		if dim.isHierarchical {
			// check the type of hierarchy
			hierarchy, err := hc.GetHierarchy(hierarchyId)
			if err != nil {
				return nil, err
			}
			dim.hierarchyType = hierarchy.Type
		}
		result = append(result, &dim)
	}

	return result, nil
}

// getHeaders returns the headers matching the values returned from getValues
func (d *Dimension) getHeaders() []string {
	var h []string
	h = append(h, fmt.Sprintf("Dimension_%d_Name", d.dimensionIndex))
	if d.isHierarchical {
		h = append(h, fmt.Sprintf("Dimension_%d_Hierarchy", d.dimensionIndex))
		h = append(h, fmt.Sprintf("Dimension_%d_Code", d.dimensionIndex))
		if d.hierarchyType != "time" {
			h = append(h, fmt.Sprintf("Dimension_%d_Value", d.dimensionIndex))
		}
	} else {
		h = append(h, fmt.Sprintf("Dimension_%d_Value", d.dimensionIndex))
	}
	return h
}

// getValues returns for hierarchical dimensions:
//   dimension name, hierarchy id, code, value (value is excluded for time hierarchies)
// for non-hierarchical dimensions:
//   dimension name, value
func (d *Dimension) getValues(row []string, requestId string) []string {
	var v []string
	v = append(v, d.name)
	if d.isHierarchical {
		v = append(v, row[d.columnIndex+HIERARCHY_ID_OFFSET])
		v = append(v, row[d.columnIndex+DIMENSION_VALUE_OFFSET])
		if d.hierarchyType != "time" {
			v = append(v, d.getHierarchyValue(row, requestId))
		}
	} else {
		v = append(v, row[d.columnIndex+DIMENSION_VALUE_OFFSET])
	}
	return v
}

// getHierarchyValue
func (d *Dimension) getHierarchyValue(row []string, requestId string) string {
	hierarchyId := row[d.columnIndex+HIERARCHY_ID_OFFSET]
	code := row[d.columnIndex+DIMENSION_VALUE_OFFSET]
	value, err := d.hc.GetHierarchyValue(hierarchyId, code)
	if err != nil {
		log.ErrorC(requestId, err, log.Data{"hierarchyId": hierarchyId, "code": code, "row": row})
		return ""
	}
	return value
}

func (p *Transformer) Transform(r io.Reader, w io.Writer, hc hierarchy.HierarchyClient, requestId string) error {

	csvReader, csvWriter := csv.NewReader(r), csv.NewWriter(w)
	defer csvWriter.Flush()

	lineCounter := 0

	// ignore the headers in the first line
	originalHeaders, err := csvReader.Read()
	if err != nil {
		log.ErrorC(requestId, err, log.Data{"message": "Unable to read header row"})
		return err
	}

	// read the first row
	row, err := csvReader.Read()
	if err == io.EOF {
		// no content - write the header row and quit
		csvWriter.Write(originalHeaders)
		return nil
	}
	if err != nil {
		log.ErrorC(requestId, err, log.Data{"message": "Unable to read first row"})
		return err
	}
	// identify the dimensions
	dimensions, err := getDimensions(row, hc)
	if err != nil {
		log.ErrorC(requestId, err, log.Data{"message": "Unable to get dimensions"})
		return err
	}
	// write the headers
	var headers []string
	headers = append(headers, "Observation")
	headers = append(headers, "Data_Marking")
	headers = append(headers, "Observation_Type_Value")
	for _, dim := range dimensions {
		headers = append(headers, dim.getHeaders()...)
	}
	csvWriter.Write(headers)

	// write each row
	rowIndex := 2
csvLoop:
	for {
		// write the row
		var output []string
		output = append(output, row[:3]...)
		for _, dim := range dimensions {
			output = append(output, dim.getValues(row, requestId)...)
		}
		csvWriter.Write(output)
		// get the next row
		rowIndex++
		row, err = csvReader.Read()
		if err != nil {
			if err == io.EOF {
				log.DebugC(requestId, "Finished transformation", log.Data{"rowsProcessed": rowIndex - 1})
				break csvLoop
			} else {
				log.ErrorC(requestId, err, log.Data{"message": fmt.Sprintf("Unable to read row %d", rowIndex)})
				return err
			}
		}

		lineCounter++
	}
	return nil
}
