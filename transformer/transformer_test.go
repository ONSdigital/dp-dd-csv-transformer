package transformer_test

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"encoding/csv"
	"errors"
	"io"

	"github.com/ONSdigital/dp-dd-csv-transformer/hierarchy"
	"github.com/ONSdigital/dp-dd-csv-transformer/transformer"
	. "github.com/smartystreets/goconvey/convey"
)

type mockHierarchyClient struct {
	timeHierarchies  map[string]bool
	errorHierarchies map[string]bool
	errorCodes       map[string]bool
}

func createMockHierarchyClient(timeHierarchies []string, errorHierarchies []string, errorCodes []string) mockHierarchyClient {
	client := mockHierarchyClient{}
	client.timeHierarchies = make(map[string]bool)
	for _, h := range timeHierarchies {
		client.timeHierarchies[h] = true
	}
	client.errorHierarchies = make(map[string]bool)
	for _, h := range errorHierarchies {
		client.errorHierarchies[h] = true
	}
	client.errorCodes = make(map[string]bool)
	for _, c := range errorCodes {
		client.errorCodes[c] = true
	}
	return client
}

func (c mockHierarchyClient) GetHierarchy(hierarchyId string) (*hierarchy.Hierarchy, error) {
	if c.errorHierarchies[hierarchyId] {
		return nil, errors.New("Error getting hierarchy")
	}
	var h hierarchy.Hierarchy
	h.ID = hierarchyId
	if c.timeHierarchies[hierarchyId] {
		h.Type = "time"
	} else {
		h.Type = "other"
	}
	return &h, nil
}

func (c mockHierarchyClient) GetHierarchyValue(hierarchyId string, entryCode string) (string, error) {
	if c.errorCodes[entryCode] {
		return "", errors.New("Error getting entry")
	}
	return "Value for " + entryCode, nil
}

func TestProcessor(t *testing.T) {

	Convey("Given a processor pointing to a local csv file", t, func() {

		var Processor = transformer.NewTransformer()

		Convey("When all hierarchies are found in AF001EW_v3_small", func() {
			mockClient := createMockHierarchyClient([]string{}, []string{}, []string{})
			inputFile := openFile("../sample_csv/AF001EW_v3_small.csv", "Error loading input file. Does it exist? ")
			outputFile := createFileInBuildDir("transformed-1.csv", "Error creating output file.")
			err := Processor.Transform(inputFile, outputFile, mockClient, "test")
			So(err, ShouldBeNil)
			rows, columns := countLinesAndColumnsInFile(outputFile.Name())
			So(rows, ShouldEqual, 13)
			// expected columns = 3 (base data) + 4 (geog) + 2 (sex) + 2 (age) + 2 (residence)
			So(columns, ShouldEqual, 13)
		})

		Convey("When all hierarchies are found in Open-Data-v3", func() {
			mockClient := createMockHierarchyClient([]string{"time"}, []string{}, []string{})
			inputFile := openFile("../sample_csv/Open-Data-v3.csv", "Error loading input file. Does it exist? ")
			outputFile := createFileInBuildDir("transformed-2.csv", "Error creating output file.")
			err := Processor.Transform(inputFile, outputFile, mockClient, "test")
			So(err, ShouldBeNil)
			rows, columns := countLinesAndColumnsInFile(outputFile.Name())
			So(rows, ShouldEqual, 277)
			// expected columns = 3 (base data) + 4 (geog) + 3 (year) + 4 (NACE) + 4 (Prodcom)
			So(columns, ShouldEqual, 18)
		})

		Convey("Should return an error if a hierarchy cannot be found", func() {
			mockClient := createMockHierarchyClient([]string{}, []string{"time"}, []string{})
			inputFile := openFile("../sample_csv/Open-Data-v3.csv", "Error loading input file. Does it exist? ")
			outputFile := createFileInBuildDir("transformed-3.csv", "Error creating output file.")
			err := Processor.Transform(inputFile, outputFile, mockClient, "test")
			So(err, ShouldNotBeNil)
		})

		Convey("Should log and continue processing if a hierarchy entry cannot be found", func() {
			mockClient := createMockHierarchyClient([]string{"time"}, []string{}, []string{"K04000001"})
			inputFile := openFile("../sample_csv/Open-Data-v3.csv", "Error loading input file. Does it exist? ")
			outputFile := createFileInBuildDir("transformed-4.csv", "Error creating output file.")
			err := Processor.Transform(inputFile, outputFile, mockClient, "test")
			So(err, ShouldBeNil)
			rows, columns := countLinesAndColumnsInFile(outputFile.Name())
			So(rows, ShouldEqual, 277)
			// expected columns = 3 (base data) + 4 (geog) + 3 (year) + 4 (NACE) + 4 (Prodcom)
			So(columns, ShouldEqual, 18)
		})
	})

}

func countLinesAndColumnsInFile(fileLocation string) (int, int) {
	finalFile, err := os.Open(fileLocation)
	if err != nil {
		fmt.Println("Error reading output file", err.Error())
		panic(err)
	}
	counter := 0
	columns := -1
	reader := csv.NewReader(bufio.NewReader(finalFile))
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return counter, columns
			}
			panic(err)
		}
		columns = len(row)
		if columns == 19 {
			fmt.Println(row)
		}
		counter++
	}
}

func openFile(fileLocation string, errorMsg string) *os.File {
	file, err := os.Open(fileLocation)
	if err != nil {
		fmt.Println(errorMsg, err.Error())
		panic(err)
	}
	return file
}

func createFileInBuildDir(fileName string, errorMsg string) *os.File {
	if _, err := os.Stat("../build"); os.IsNotExist(err) {
		os.Mkdir("../build", os.ModePerm)
	}

	file, err := os.Create("../build/" + fileName)

	if err != nil {
		fmt.Println(errorMsg, err.Error())
		panic(err)
	}

	return file
}

func main() {
	var Processor = transformer.NewTransformer()

	inputFile := openFile("../sample_csv/Open-Data-v3.csv", "Error loading input file. Does it exist? ")
	outputFile := createFileInBuildDir("transformed-Open-Data-1.csv", "Error creating output file.")
	Processor.Transform(inputFile, outputFile, hierarchy.NewHierarchyClient(), "test")

	inputFile = openFile("../sample_csv/AF001EW_v3_small.csv", "Error loading input file. Does it exist? ")
	outputFile = createFileInBuildDir("transformed-AF001EW_v3_small_1.csv", "Error creating output file.")
	Processor.Transform(inputFile, outputFile, hierarchy.NewHierarchyClient(), "test")

}
