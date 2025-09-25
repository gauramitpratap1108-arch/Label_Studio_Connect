import logging
import os
from typing import Annotated, Optional
import requests
import vtk
import tempfile
import slicer
from slicer.i18n import tr as _
from slicer.i18n import translate
from slicer.ScriptedLoadableModule import *
from slicer.util import VTKObservationMixin
from slicer.parameterNodeWrapper import (
    parameterNodeWrapper,
    WithinRange,
)
from PIL import Image
import time

from slicer import vtkMRMLScalarVolumeNode
import numpy as np
import uuid
from datetime import datetime, timezone
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import zipfile

#
# Label_Studio_Conn
#


class Label_Studio_Conn(ScriptedLoadableModule):
    """Uses ScriptedLoadableModule base class, available at:
    https://github.com/Slicer/Slicer/blob/main/Base/Python/slicer/ScriptedLoadableModule.py
    """

    def __init__(self, parent):
        ScriptedLoadableModule.__init__(self, parent)
        self.parent.title = _("Label_Studio_Conn")  # TODO: make this more human readable by adding spaces
        # TODO: set categories (folders where the module shows up in the module selector)
        self.parent.categories = [translate("qSlicerAbstractCoreModule", "Examples")]
        self.parent.dependencies = []  # TODO: add here list of module names that this module requires
        self.parent.contributors = ["John Doe (AnyWare Corp.)"]  # TODO: replace with "Firstname Lastname (Organization)"
        # TODO: update with short description of the module and a link to online module documentation
        # _() function marks text as translatable to other languages
        self.parent.helpText = _("""
This is an example of scripted loadable module bundled in an extension.
See more information in <a href="https://github.com/organization/projectname#Label_Studio_Conn">module documentation</a>.
""")
        # TODO: replace with organization, grant and thanks
        self.parent.acknowledgementText = _("""
This file was originally developed by Jean-Christophe Fillion-Robin, Kitware Inc., Andras Lasso, PerkLab,
and Steve Pieper, Isomics, Inc. and was partially funded by NIH grant 3P41RR013218-12S1.
""")

        # Additional initialization step after application startup is complete
        slicer.app.connect("startupCompleted()", registerSampleData)


#
# Register sample data sets in Sample Data module
#


def registerSampleData():
    """Add data sets to Sample Data module."""
    # It is always recommended to provide sample data for users to make it easy to try the module,
    # but if no sample data is available then this method (and associated startupCompeted signal connection) can be removed.

    import SampleData

    iconsPath = os.path.join(os.path.dirname(__file__), "Resources/Icons")

    # To ensure that the source code repository remains small (can be downloaded and installed quickly)
    # it is recommended to store data sets that are larger than a few MB in a Github release.

    # Label_Studio_Conn1
    SampleData.SampleDataLogic.registerCustomSampleDataSource(
        # Category and sample name displayed in Sample Data module
        category="Label_Studio_Conn",
        sampleName="Label_Studio_Conn1",
        # Thumbnail should have size of approximately 260x280 pixels and stored in Resources/Icons folder.
        # It can be created by Screen Capture module, "Capture all views" option enabled, "Number of images" set to "Single".
        thumbnailFileName=os.path.join(iconsPath, "Label_Studio_Conn1.png"),
        # Download URL and target file name
        uris="https://github.com/Slicer/SlicerTestingData/releases/download/SHA256/998cb522173839c78657f4bc0ea907cea09fd04e44601f17c82ea27927937b95",
        fileNames="Label_Studio_Conn1.nrrd",
        # Checksum to ensure file integrity. Can be computed by this command:
        #  import hashlib; print(hashlib.sha256(open(filename, "rb").read()).hexdigest())
        checksums="SHA256:998cb522173839c78657f4bc0ea907cea09fd04e44601f17c82ea27927937b95",
        # This node name will be used when the data set is loaded
        nodeNames="Label_Studio_Conn1",
    )

    # Label_Studio_Conn2
    SampleData.SampleDataLogic.registerCustomSampleDataSource(
        # Category and sample name displayed in Sample Data module
        category="Label_Studio_Conn",
        sampleName="Label_Studio_Conn2",
        thumbnailFileName=os.path.join(iconsPath, "Label_Studio_Conn2.png"),
        # Download URL and target file name
        uris="https://github.com/Slicer/SlicerTestingData/releases/download/SHA256/1a64f3f422eb3d1c9b093d1a18da354b13bcf307907c66317e2463ee530b7a97",
        fileNames="Label_Studio_Conn2.nrrd",
        checksums="SHA256:1a64f3f422eb3d1c9b093d1a18da354b13bcf307907c66317e2463ee530b7a97",
        # This node name will be used when the data set is loaded
        nodeNames="Label_Studio_Conn2",
    )


#
# Label_Studio_ConnParameterNode
#


@parameterNodeWrapper
class Label_Studio_ConnParameterNode:
    """
    The parameters needed by module.

    inputVolume - The volume to threshold.
    imageThreshold - The value at which to threshold the input volume.
    invertThreshold - If true, will invert the threshold.
    thresholdedVolume - The output volume that will contain the thresholded volume.
    invertedVolume - The output volume that will contain the inverted thresholded volume.
    """

    inputVolume: vtkMRMLScalarVolumeNode
    imageThreshold: Annotated[float, WithinRange(-100, 500)] = 100
    invertThreshold: bool = False
    thresholdedVolume: vtkMRMLScalarVolumeNode
    invertedVolume: vtkMRMLScalarVolumeNode
    authenticationKey: str = ""


#
# Label_Studio_ConnWidget
#


class Label_Studio_ConnWidget(ScriptedLoadableModuleWidget, VTKObservationMixin):
    """Uses ScriptedLoadableModuleWidget base class, available at:
    https://github.com/Slicer/Slicer/blob/main/Base/Python/slicer/ScriptedLoadableModule.py
    """

    def __init__(self, parent=None) -> None:
        """Called when the user opens the module the first time and the widget is initialized."""
        ScriptedLoadableModuleWidget.__init__(self, parent)
        VTKObservationMixin.__init__(self)  # needed for parameter node observation
        self.logic = None
        self._parameterNode = None
        self._parameterNodeGuiTag = None

    def setup(self):
        ScriptedLoadableModuleWidget.setup(self)
        uiWidget = slicer.util.loadUI(self.resourcePath("UI/Label_Studio_Conn.ui"))
        self.layout.addWidget(uiWidget)
        self.ui = slicer.util.childWidgetVariables(uiWidget)
        uiWidget.setMRMLScene(slicer.mrmlScene)
        self.logic = Label_Studio_ConnLogic()

        # Connect buttons and widgets
        self.ui.applyButton.connect("clicked(bool)", self.onApplyButton)
        self.ui.importButton.connect("clicked(bool)", self.onImportButton)
        self.ui.connectButton.connect("clicked(bool)", self.onConnectButtonClicked)
        self.ui.projectSelector.connect("currentIndexChanged(int)", self.onProjectSelectionChanged)
        self.ui.exportButton.connect("clicked(bool)", self.onExportButtonClicked)


        self.initializeParameterNode()


        # Make sure parameter node is initialized (needed for module reload)
        self.initializeParameterNode()

    def cleanup(self) -> None:
        """Called when the application closes and the module widget is destroyed."""
        self.removeObservers()

    def enter(self) -> None:
        """Called each time the user opens this module."""
        # Make sure parameter node exists and observed
        self.initializeParameterNode()

    def exit(self) -> None:
        """Called each time the user opens a different module."""
        # Do not react to parameter node changes (GUI will be updated when the user enters into the module)
        if self._parameterNode:
            self._parameterNode.disconnectGui(self._parameterNodeGuiTag)
            self._parameterNodeGuiTag = None
            self.removeObserver(self._parameterNode, vtk.vtkCommand.ModifiedEvent, self._checkCanApply)

    def onSceneStartClose(self, caller, event) -> None:
        """Called just before the scene is closed."""
        # Parameter node will be reset, do not use it anymore
        self.setParameterNode(None)

    def onSceneEndClose(self, caller, event) -> None:
        """Called just after the scene is closed."""
        # If this module is shown while the scene is closed then recreate a new parameter node immediately
        if self.parent.isEntered:
            self.initializeParameterNode()

    def initializeParameterNode(self) -> None:
        """Ensure parameter node exists and observed."""
        # Parameter node stores all user choices in parameter values, node selections, etc.
        # so that when the scene is saved and reloaded, these settings are restored.

        self.setParameterNode(self.logic.getParameterNode())

        # Select default input nodes if nothing is selected yet to save a few clicks for the user
        if not self._parameterNode.inputVolume:
            firstVolumeNode = slicer.mrmlScene.GetFirstNodeByClass("vtkMRMLScalarVolumeNode")
            if firstVolumeNode:
                self._parameterNode.inputVolume = firstVolumeNode

    def setParameterNode(self, inputParameterNode: Optional[Label_Studio_ConnParameterNode]) -> None:
        """
        Set and observe parameter node.
        Observation is needed because when the parameter node is changed then the GUI must be updated immediately.
        """

        if self._parameterNode:
            self._parameterNode.disconnectGui(self._parameterNodeGuiTag)
            self.removeObserver(self._parameterNode, vtk.vtkCommand.ModifiedEvent, self._checkCanApply)
        self._parameterNode = inputParameterNode
        if self._parameterNode:
            # Note: in the .ui file, a Qt dynamic property called "SlicerParameterName" is set on each
            # ui element that needs connection.
            self._parameterNodeGuiTag = self._parameterNode.connectGui(self.ui)
            self.addObserver(self._parameterNode, vtk.vtkCommand.ModifiedEvent, self._checkCanApply)
            self._checkCanApply()

    def _checkCanApply(self, caller=None, event=None) -> None:
        if self._parameterNode and self._parameterNode.inputVolume and self._parameterNode.thresholdedVolume:
            self.ui.applyButton.toolTip = _("Compute output volume")
            self.ui.applyButton.enabled = True
        else:
            self.ui.applyButton.toolTip = _("Select input and output volume nodes")
            self.ui.applyButton.enabled = False

    def onApplyButton(self) -> None:
        """Run processing when user clicks "Apply" button."""
        with slicer.util.tryWithErrorDisplay(_("Failed to compute results."), waitCursor=True):
            # Compute output
            self.logic.process(self.ui.inputSelector.currentNode(), self.ui.outputSelector.currentNode(),
                               self.ui.imageThresholdSliderWidget.value, self.ui.invertOutputCheckBox.checked)

            # Compute inverted output (if needed)
            if self.ui.invertedOutputSelector.currentNode():
                # If additional output volume is selected then result with inverted threshold is written there
                self.logic.process(self.ui.inputSelector.currentNode(), self.ui.invertedOutputSelector.currentNode(),
                                   self.ui.imageThresholdSliderWidget.value, not self.ui.invertOutputCheckBox.checked, showResult=False)
    
    def clearLoadedData(self):
        # Remove segmentation nodes (segmentations and masks)
        for segNode in slicer.util.getNodesByClass("vtkMRMLSegmentationNode"):
            slicer.mrmlScene.RemoveNode(segNode)
            
        # Remove label map nodes (segmentation label layers)
        for labelNode in slicer.util.getNodesByClass("vtkMRMLLabelMapVolumeNode"):
            slicer.mrmlScene.RemoveNode(labelNode)
        
        # Remove scalar volume nodes (base image volumes)
        for volumeNode in slicer.util.getNodesByClass("vtkMRMLScalarVolumeNode"):
            slicer.mrmlScene.RemoveNode(volumeNode)

    
    def onConnectButtonClicked(self):
        api_key = self.ui.authenticationKeyInput.text
        if not api_key:
            slicer.util.errorDisplay("Please enter the Label Studio authentication key.")
            return
        self.fetchProjects(api_key)

    def fetchProjects(self, api_key):
        base_url = "https://annotations-dev.oneforma2.com"
        headers = {"Authorization": f"Token {api_key}"}
        projects_url = f"{base_url}/api/projects"
        try:
            response = requests.get(projects_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                projects = data.get("results", []) 
                self.ui.projectSelector.clear()
                for project in projects:
                    title = project.get("title", "Unnamed")
                    pid = project.get("id")
                    self.ui.projectSelector.addItem(title, pid)
            else:
                slicer.util.errorDisplay(f"Failed to fetch projects: HTTP {response.status_code}")
        except Exception as e:
            slicer.util.errorDisplay(f"Exception fetching projects: {e}")

    def onProjectSelectionChanged(self, index):
        self.clearLoadedData()
        if index < 0:
            return
        api_key = self.ui.authenticationKeyInput.text
        if not api_key:
            slicer.util.errorDisplay("Please enter the Label Studio authentication key.")
            return
        project_id = self.ui.projectSelector.itemData(index)
        self.fetchTasks(api_key, project_id)
        self.fetchLabels(api_key, project_id)
    
    def fetchLabels(self, api_key, project_id):
        base_url = "https://annotations-dev.oneforma2.com"
        headers = {"Authorization": f"Token {api_key}"}
        detail_url = f"{base_url}/api/projects/{project_id}"
        try:
            response = requests.get(detail_url, headers=headers)
            if response.status_code == 200:
                project_detail = response.json()
                label_config = project_detail.get("label_config", "")
                labels = self.parseLabelStudioLabels(label_config)
                inputVolumeNode = self._parameterNode.inputVolume      
                self.logic.createSegmentsForLabels(labels, inputVolumeNode)
                self.ui.labelsListWidget.clear()
                for label in labels:
                    self.ui.labelsListWidget.addItem(label)
            else:
                slicer.util.errorDisplay(f"Failed to fetch project detail for labels: HTTP {response.status_code}")
        except Exception as e:
            slicer.util.errorDisplay(f"Exception fetching project labels: {e}")
    
    def parseLabelStudioLabels(self, label_config_xml):
        import xml.etree.ElementTree as ET
        labels = []
        try:
            root = ET.fromstring(label_config_xml)
            for controlTag in root.findall(".//Label"):
                label = controlTag.attrib.get('value')
                if label:
                    labels.append(label)
        except Exception as e:
            logging.error(f"Cannot parse labels from label config: {e}")
        return labels


    def fetchTasks(self, api_key, project_id):
        base_url = "https://annotations-dev.oneforma2.com"
        headers = {"Authorization": f"Token {api_key}"}
        tasks_url = f"{base_url}/api/projects/{project_id}/tasks"

        try:
            response = requests.get(tasks_url, headers=headers)
            if response.status_code == 200:
                tasks = response.json()  # This is a list
                self.ui.taskSelector.clear()
                for task in tasks:
                    task_title = task.get("title", f"Task {task.get('id', 'unknown')}")
                    self.ui.taskSelector.addItem(task_title, task.get("id"))
            else:
                slicer.util.errorDisplay(f"Failed to fetch tasks: HTTP {response.status_code}")
        except Exception as e:
            slicer.util.errorDisplay(f"Exception fetching tasks: {e}")


    def onImportButton(self):
        
        api_key = self.ui.authenticationKeyInput.text
        if not api_key:
            slicer.util.errorDisplay("Please enter the Label Studio authentication key.")
            return

        task_index = self.ui.taskSelector.currentIndex
        if task_index < 0:
            slicer.util.errorDisplay("Please select a task from the dropdown.")
            return

        task_id = self.ui.taskSelector.itemData(task_index)
        self.clearLoadedData()

        # Call logic method and get path and message
        file_path, message = self.logic.importDicomTask(api_key, str(task_id))
        if file_path is None:
            slicer.util.errorDisplay(message)
        else:
            slicer.util.infoDisplay(message)
        
        loaded_volume = slicer.mrmlScene.GetFirstNodeByClass("vtkMRMLScalarVolumeNode")
        if loaded_volume and self._parameterNode:
            self._parameterNode.inputVolume = loaded_volume

        # Fetch & create segments for new volume/labels
        project_index = self.ui.projectSelector.currentIndex
        if project_index >= 0:
            project_id = self.ui.projectSelector.itemData(project_index)
            api_key = self.ui.authenticationKeyInput.text
            self.fetchLabels(api_key, project_id)


    
    def onExportButtonClicked(self):
        segmentationNode = slicer.mrmlScene.GetFirstNodeByClass('vtkMRMLSegmentationNode')
        if not segmentationNode:
            slicer.util.errorDisplay("Segmentation node not found in the scene.")
            return

        task_index = self.ui.taskSelector.currentIndex
        if task_index < 0:
            slicer.util.errorDisplay("Please select a task before exporting")
            return
        task_id = self.ui.taskSelector.itemData(task_index)

        api_key = self.ui.authenticationKeyInput.text
        if not api_key:
            slicer.util.errorDisplay("Please enter the Label Studio authentication key.")
            return

        # Add here to get current project ID from UI selector
        project_index = self.ui.projectSelector.currentIndex
        if project_index < 0:
            slicer.util.errorDisplay("Please select a project before exporting")
            return
        project_id = self.ui.projectSelector.itemData(project_index)

        # Now call export with project_id as well
        success = self.logic.exportBrushlabelsToLabelStudio(segmentationNode, project_id, task_id, api_key)
        if success:
            slicer.util.infoDisplay("Annotations exported successfully to Label Studio.")
        else:
            slicer.util.errorDisplay("Failed to export annotations.")








#
# Label_Studio_ConnLogic
#


class Label_Studio_ConnLogic(ScriptedLoadableModuleLogic):
    """This class should implement all the actual
    computation done by your module.  The interface
    should be such that other python code can import
    this class and make use of the functionality without
    requiring an instance of the Widget.
    Uses ScriptedLoadableModuleLogic base class, available at:
    https://github.com/Slicer/Slicer/blob/main/Base/Python/slicer/ScriptedLoadableModule.py
    """

    def __init__(self) -> None:
        """Called when the logic class is instantiated. Can be used for initializing member variables."""
        ScriptedLoadableModuleLogic.__init__(self)

    def getParameterNode(self):
        return Label_Studio_ConnParameterNode(super().getParameterNode())

    def process(self,
                inputVolume: vtkMRMLScalarVolumeNode,
                outputVolume: vtkMRMLScalarVolumeNode,
                imageThreshold: float,
                invert: bool = False,
                showResult: bool = True) -> None:
        """
        Run the processing algorithm.
        Can be used without GUI widget.
        :param inputVolume: volume to be thresholded
        :param outputVolume: thresholding result
        :param imageThreshold: values above/below this threshold will be set to 0
        :param invert: if True then values above the threshold will be set to 0, otherwise values below are set to 0
        :param showResult: show output volume in slice viewers
        """

        if not inputVolume or not outputVolume:
            raise ValueError("Input or output volume is invalid")

        import time

        startTime = time.time()
        logging.info("Processing started")

        # Compute the thresholded output volume using the "Threshold Scalar Volume" CLI module
        cliParams = {
            "InputVolume": inputVolume.GetID(),
            "OutputVolume": outputVolume.GetID(),
            "ThresholdValue": imageThreshold,
            "ThresholdType": "Above" if invert else "Below",
        }
        cliNode = slicer.cli.run(slicer.modules.thresholdscalarvolume, None, cliParams, wait_for_completion=True, update_display=showResult)
        # We don't need the CLI module node anymore, remove it to not clutter the scene with it
        slicer.mrmlScene.RemoveNode(cliNode)

        stopTime = time.time()
        logging.info(f"Processing completed in {stopTime-startTime:.2f} seconds")
    
    # def importDicomTask(self, api_key: str, task_id: str):
    #     base_url = "https://annotations-dev.oneforma2.com"
    #     headers = {"Authorization": f"Token {api_key}"}
    #     task_url = f"{base_url}/api/tasks/{task_id}"

    #     try:
    #         response = requests.get(task_url, headers=headers)
    #         if response.status_code != 200:
    #             logging.error(f"Failed to fetch task: HTTP {response.status_code}")
    #             return None
    #         task_json = response.json()

    #         # Get the DICOM or ZIP URL from the task data
    #         file_url = task_json['data'].get('dicom_url') or task_json['data'].get('image')
    #         if not file_url:
    #             logging.error("No DICOM/ZIP URL found in task data")
    #             return None

    #         if file_url.lower().endswith('.zip'):
    #             # If the URL is a ZIP archive, download and extract it
    #             dicom_dir = self.download_and_extract_zip(file_url)
    #             if not dicom_dir:
    #                 logging.error("Failed to download or extract ZIP file")
    #                 return None
    #             loaded_node = self.loadDicomDirectoryIntoSlicer(dicom_dir)
    #         else:
    #             # Otherwise, assume it's a single DICOM file and download it
    #             downloaded_file = self.download_dicomsas_file(file_url)
    #             if not downloaded_file:
    #                 logging.error("Failed to download DICOM file")
    #                 return None
    #             loaded_node = self.loadDicomFileIntoSlicer(downloaded_file)

    #         if not loaded_node:
    #             logging.error("Failed to load DICOM into Slicer")
    #             return None

    #         return loaded_node

    #     except Exception as e:
    #         logging.error(f"Exception during DICOM task import: {e}")
    #         return None


    # def downloadfilewithsas(self, fileurl: str) -> Optional[str]:
    #     """
    #     Downloads a file from the given URL, generating a SAS token if needed,
    #     saves it locally as a temporary file, and returns the local file path.

    #     Parameters:
    #         fileurl (str): The URL of the file to download, potentially Azure blob URL.

    #     Returns:
    #         str: The local file path where the file is saved, or None if failed.
    #     """
    #     try:
    #         # Generate SAS token for the blob URL if needed
    #         sastoken = self.generate_sas_token_for_blob(fileurl)
    #         if not sastoken:
    #             logging.error("SAS token generation failed")
    #             return None
            
    #         # Remove existing query parameters from URL and append SAS token
    #         base_url = fileurl.split('?')[0]
    #         download_url = f"{base_url}?{sastoken}"

    #         # Create a temporary file to save the download
    #         suffix = os.path.splitext(base_url)[1] or ""
    #         temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)

    #         # Download the file in chunks
    #         with requests.get(download_url, stream=True, timeout=60) as response:
    #             response.raise_for_status()
    #             for chunk in response.iter_content(chunk_size=8192):
    #                 if chunk:
    #                     temp_file.write(chunk)
    #         temp_file.close()

    #         logging.info(f"Downloaded file saved to {temp_file.name}")
    #         return temp_file.name

    #     except Exception as e:
    #         logging.error(f"Error downloading file: {e}")
    #         return None


    # def importDicomTask(self, apikey: str, taskid: str):
    #     baseurl = "https://annotations-dev.oneforma2.com"
    #     headers = {"Authorization": f"Token {apikey}"}
    #     taskurl = f"{baseurl}/api/tasks/{taskid}"

    #     try:
    #         response = requests.get(taskurl, headers=headers)
    #         if response.status_code != 200:
    #             logging.error(f"Failed to fetch task, HTTP {response.status_code}")
    #             return None, "Failed to fetch task"

    #         taskjson = response.json()
    #         fileurl = taskjson.get("data", {}).get("dicomUrl") or taskjson.get("data", {}).get("image")
    #         if not fileurl:
    #             logging.error("No file URL found in task data")
    #             return None, "No file URL found"

    #         # Check if URL is a ZIP archive
    #         if fileurl.lower().endswith('.zip'):
    #             # Download ZIP and return local path (manual loading)
    #             downloaded_file = self.downloadfilewithsas(fileurl)
    #             if not downloaded_file:
    #                 logging.error("Failed to download ZIP file")
    #                 return None, "Failed to download ZIP file"
    #             print(f"Downloaded ZIP file path: {downloaded_file}")
    #             return downloaded_file, f"Ready to use, ZIP file saved to your local system: {downloaded_file}"
    #         else:
    #             # Assume single DICOM file, download and load directly into Slicer
    #             downloaded_file = self.download_dicomsas_file(fileurl)
    #             if not downloaded_file:
    #                 logging.error("Failed to download DICOM file")
    #                 return None, "Failed to download DICOM file"

    #             loaded_node = self.loadDicomFileIntoSlicer(downloaded_file)
    #             if not loaded_node:
    #                 logging.error("Failed to load DICOM file into Slicer")
    #                 return None, "Failed to load DICOM file into Slicer"

    #             print(f"Downloaded and loaded DICOM file: {downloaded_file}")
    #             return loaded_node, f"DICOM file loaded successfully: {downloaded_file}"

    #     except Exception as e:
    #         logging.error(f"Exception during task import: {e}")
    #         return None, f"Exception occurred: {str(e)}"

    def importDicomTask(self, apikey: str, taskid: str):
        baseurl = "https://annotations-dev.oneforma2.com"
        headers = {"Authorization": f"Token {apikey}"}
        taskurl = f"{baseurl}/api/tasks/{taskid}"

        try:
            response = requests.get(taskurl, headers=headers)
            if response.status_code != 200:
                logging.error(f"Failed to fetch task, HTTP {response.status_code}")
                return None, "Failed to fetch task"

            taskjson = response.json()
            fileurl = taskjson.get("data", {}).get("image")
            if not fileurl:
                logging.error("No DICOM file URL found in task data under 'dicom' key")
                return None, "No DICOM file URL found"

            # Download the DICOM file
            downloaded_file = self.download_dicomsas_file(fileurl)
            if not downloaded_file:
                logging.error("Failed to download DICOM file")
                return None, "Failed to download DICOM file"

            # Load the DICOM file into Slicer
            loaded_node = self.loadDicomFileIntoSlicer(downloaded_file)
            if not loaded_node:
                logging.error("Failed to load DICOM file into Slicer")
                return None, "Failed to load DICOM file into Slicer"

            print(f"Downloaded and loaded DICOM file: {downloaded_file}")
            return loaded_node, f"DICOM file loaded successfully: {downloaded_file}"

        except Exception as e:
            logging.error(f"Exception during task import: {e}")
            return None, f"Exception occurred: {str(e)}"
    
    def loadDicomFileIntoSlicer(self, dicom_file_path):
        try:
            if not os.path.exists(dicom_file_path):
                logging.error("DICOM file does not exist")
                return None
            loadedNode = slicer.util.loadVolume(dicom_file_path)
            if loadedNode:
                slicer.util.setSliceViewerLayers(background=loadedNode)
                logging.info(f"Loaded DICOM file: {loadedNode.GetName()}")
                return loadedNode
            else:
                logging.error("Failed to load DICOM file into Slicer")
                return None
        except Exception as e:
            logging.error(f"Error loading DICOM file: {e}")
            return None



    # def download_and_extract_zip(self, zip_url):
    #     import tempfile
    #     import zipfile
    #     sas_token = self.generate_sas_token_for_blob(zip_url)
    #     if not sas_token:
    #         logging.error("SAS token generation failed")
    #         return None

    #     download_url = f"{zip_url.split('?')[0]}?{sas_token}"

    #     temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    #     try:
    #         with requests.get(download_url, stream=True, timeout=60) as r:
    #             r.raise_for_status()
    #             for chunk in r.iter_content(chunk_size=8192):
    #                 if chunk:
    #                     temp_zip.write(chunk)
    #         temp_zip.close()
    #         extract_dir = tempfile.mkdtemp()
    #         with zipfile.ZipFile(temp_zip.name, 'r') as zip_ref:
    #             zip_ref.extractall(extract_dir)
    #         os.unlink(temp_zip.name)
    #         return extract_dir
    #     except Exception as e:
    #         logging.error(f"Error downloading/extracting ZIP: {e}")
    #         return None

    # def loadDicomDirectoryIntoSlicer(self, dicom_dir):
    #     try:
    #         loadedNodes = slicer.util.loadStudy(dicom_dir)
    #         if loadedNodes:
    #             slicer.util.setSliceViewerLayers(background=loadedNodes[0])
    #             logging.info(f"Loaded DICOM directory: {dicom_dir}")
    #             return loadedNodes[0]
    #         else:
    #             logging.error("Failed to load DICOM directory into Slicer")
    #             return None
    #     except Exception as e:
    #         logging.error(f"Error loading DICOM directory: {e}")
    #         return None

    def loadDicomFileIntoSlicer(self, dicom_file_path):
        try:
            if not os.path.exists(dicom_file_path):
                logging.error("DICOM file does not exist")
                return None
            loadedNode = slicer.util.loadVolume(dicom_file_path)
            if loadedNode:
                slicer.util.setSliceViewerLayers(background=loadedNode)
                logging.info(f"Loaded DICOM file: {loadedNode.GetName()}")
                return loadedNode
            else:
                logging.error("Failed to load DICOM file into Slicer")
                return None
        except Exception as e:
            logging.error(f"Error loading DICOM file: {e}")
            return None


## This code will be applicable when we export from blob storage as of now we are not using it
    ##############################################################################################################

    # Static storage account info (ideally, store securely, not as plain text)
    
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    account_key = os.getenv("AZURE_STORAGE_KEY", "")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER", "dicom-image")

    def extract_blob_info_from_url(self, blob_url):
        parsed_url = urlparse(blob_url)
        account_name = parsed_url.netloc.split('.')[0]
        path_parts = parsed_url.path.lstrip('/').split('/', 1)
        container_name = path_parts[0]
        blob_name = path_parts[1] if len(path_parts) > 1 else ''
        return account_name, container_name, blob_name

    def generate_sas_token_for_blob(self, blob_url):
        try:
            account_name_extracted, container_name_extracted, blob_name = self.extract_blob_info_from_url(blob_url)
            # Validate extracted container name matches expected container if needed
            sas_token = generate_blob_sas(
                account_name=account_name_extracted,
                container_name=container_name_extracted,
                blob_name=blob_name,
                account_key=self.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(hours=10)
            )
            return sas_token
        except Exception as e:
            logging.error(f"Failed to generate SAS token: {e}")
            return None


    def download_file_with_sas(self, file_url):
        try:
            sas_token = self.generate_sas_token_for_blob(file_url)
            if not sas_token:
                logging.error("SAS token generation failed")
                return None
            download_url = f"{file_url.split('?')[0]}?{sas_token}"

            # Extract path without query to get extension safely
            path = urlparse(file_url).path  # e.g. '/dicom-zip/2_skull_ct.zip'
            ext = os.path.splitext(path)[1] or ".file"

            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
            with requests.get(download_url, stream=True, timeout=60) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
            temp_file.close()
            logging.info(f"Downloaded file saved to: {temp_file.name}")
            return temp_file.name
        except Exception as e:
            logging.error(f"Error downloading file: {e}")
            return None
    
    def loadDicomZipIntoSlicer(self, zip_file_path):
        

        temp_dir = tempfile.mkdtemp()
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
        except Exception as e:
            logging.error(f"Failed to extract ZIP file: {e}")
            return None

        try:
            if not hasattr(slicer.modules, 'dicom'):
                logging.error("DICOM module is not available")
                return None

            slicer.cli.run(slicer.modules.dicom, None, {'directory': temp_dir}, wait_for_completion=True)

            db = slicer.dicomDatabase

            # Wait briefly for database update
            time.sleep(1)

            series_uids = []
            for file in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, file)
                series_uids.extend(db.seriesForFile(file_path))

            # Deduplicate and filter valid series UID strings
            series_uids = list(filter(lambda s: isinstance(s, str) and len(s) > 0, set(series_uids)))

            if not series_uids:
                logging.error("No DICOM series found after import.")
                return None

            # Load first valid series UID
            loaded_node = slicer.util.loadVolume(series_uids[0])
            slicer.util.setSliceViewerLayers(background=loaded_node)
            logging.info(f"Loaded DICOM series from extracted ZIP folder: {temp_dir}")
            return loaded_node

        except Exception as e:
            logging.error(f"Error loading DICOM series from folder: {e}")
            return None

    def download_dicomsas_file(self, blob_url):
        try:
            sas_token = self.generate_sas_token_for_blob(blob_url)
            if not sas_token:
                logging.error("SAS token generation failed")
                return None

            # Combine URL with SAS token
            download_url = f"{blob_url.split('?')[0]}?{sas_token}"  # Remove existing query, append generated SAS
            print(f"Downloading DICOM from: {download_url}")

            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".dcm")
            with requests.get(download_url, stream=True, timeout=30) as response:
                response.raise_for_status()
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
            temp_file.close()
            return temp_file.name
        except Exception as e:
            logging.error(f"Error downloading DICOM file: {e}")
            return None

###############################################################################################################################################
        

    def createSegmentsForLabels(self, label_list, volumeNode=None):
        segmentationNode = slicer.mrmlScene.GetFirstNodeByClass('vtkMRMLSegmentationNode')
        if not segmentationNode:
            segmentationNode = slicer.mrmlScene.AddNewNodeByClass('vtkMRMLSegmentationNode')
            if volumeNode:
                segmentationNode.SetReferenceImageGeometryParameterFromVolumeNode(volumeNode)
        
        segmentationNode.GetSegmentation().RemoveAllSegments()
        unique_labels = list(dict.fromkeys(label_list))
        for label in unique_labels:
            segmentation = segmentationNode.GetSegmentation()
            # Add empty segment returns segment ID
            segment_id = segmentation.AddEmptySegment()
            segment = segmentation.GetSegment(segment_id)
            # Explicitly set the segment name to your label (important!)
            print(f"Adding segment with label: {label}")
            segment.SetName(label)
            # Optionally set random color
            import random
            color = [random.random(), random.random(), random.random()]
            segment.SetColor(color)
        print(f"Segments now in node: {[segmentationNode.GetSegmentation().GetNthSegment(i).GetName() for i in range(segmentationNode.GetSegmentation().GetNumberOfSegments())]}")
        return segmentationNode

    def numpy_rle_encode(self, mask):
        print(f"Encoding mask with shape: {mask.shape}")
        pixels = mask.flatten()
        rle = []
        prev_pixel = pixels[0]
        count = 1
        for pixel in pixels[1:]:
            if pixel == prev_pixel:
                count += 1
            else:
                rle.append(count)
                count = 1
                prev_pixel = pixel
        rle.append(count)
        print(f"RLE length: {len(rle)}, Sample: {rle[:20]}")
        return rle

    def exportBrushlabelsToLabelStudio(self, segmentationNode, project_id, task_id, api_key):
        segmentation = segmentationNode.GetSegmentation()
        new_annotation_results = []
        for i in range(segmentation.GetNumberOfSegments()):
            segmentId = segmentation.GetNthSegmentID(i)
            mask = slicer.util.arrayFromSegmentBinaryLabelmap(segmentationNode, segmentId)

            mask_2d = mask
            original_height, original_width = mask_2d.shape[-2], mask_2d.shape[-1]
            rle = self.numpy_rle_encode(mask.astype(np.uint8))
            brush_label = segmentation.GetSegment(segmentId).GetName()
            new_annotation_results.append({
                "original_width": original_width,
                "original_height": original_height,
                "image_rotation": 0,
                "value": {
                    "format": "rle",
                    "rle": rle,
                    "brushlabels": [brush_label]
                },
                "id": str(uuid.uuid4())[:12],
                "from_name": "tag",
                "to_name": "image",
                "type": "brushlabels",
                "origin": "manual"
            })

        headers = {
            "Authorization": f"Token {api_key}",
            "Content-Type": "application/json"
        }
        task_api_url = f"https://annotations-dev.oneforma2.com/api/tasks/{task_id}"

        # Get existing task data
        response = requests.get(task_api_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to get existing task data: {response.status_code} {response.text}")
            return False
        existing_task = response.json()
        existing_data = existing_task.get("data", {})

        # Extract existing slicer_annotation (could be dict or already an annotations array)
        existing_slicer_annotation = existing_data.get("slicer_annotation")

        # Prepare the new annotation object
        new_annotation_obj = {
            "lead_time": 10.0,
            "result": new_annotation_results,
            "draft_id": 0,
            "parent_annotation": None,
            "parent_prediction": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "project": project_id
        }

        # Combine as list if existing annotation present, else just new
        if existing_slicer_annotation:
            if isinstance(existing_slicer_annotation, dict) and "annotations" in existing_slicer_annotation:
                # Already contains multiple annotations
                combined_annotations = existing_slicer_annotation["annotations"] + [new_annotation_obj]
            else:
                # Single annotation present, convert to array
                combined_annotations = [existing_slicer_annotation, new_annotation_obj]
            updated_slicer_annotation = {
                "annotations": combined_annotations
            }
        else:
            # No existing annotation
            updated_slicer_annotation = new_annotation_obj

        # Update the data dictionary
        updated_data = existing_data.copy()
        updated_data["slicer_annotation"] = updated_slicer_annotation

        patch_payload = {"data": updated_data}

        # PATCH back to the task
        patch_response = requests.patch(task_api_url, headers=headers, json=patch_payload)

        print(f"PATCH {task_api_url}")
        print(f"Payload: {patch_payload}")
        print(f"API response: {patch_response.status_code} {patch_response.text}")

        return patch_response.status_code in (200, 204)


#
# Label_Studio_ConnTest
#


class Label_Studio_ConnTest(ScriptedLoadableModuleTest):
    """
    This is the test case for your scripted module.
    Uses ScriptedLoadableModuleTest base class, available at:
    https://github.com/Slicer/Slicer/blob/main/Base/Python/slicer/ScriptedLoadableModule.py
    """

    def setUp(self):
        """Do whatever is needed to reset the state - typically a scene clear will be enough."""
        slicer.mrmlScene.Clear()

    def runTest(self):
        """Run as few or as many tests as needed here."""
        self.setUp()
        self.test_Label_Studio_Conn1()

    def test_Label_Studio_Conn1(self):
        """Ideally you should have several levels of tests.  At the lowest level
        tests should exercise the functionality of the logic with different inputs
        (both valid and invalid).  At higher levels your tests should emulate the
        way the user would interact with your code and confirm that it still works
        the way you intended.
        One of the most important features of the tests is that it should alert other
        developers when their changes will have an impact on the behavior of your
        module.  For example, if a developer removes a feature that you depend on,
        your test should break so they know that the feature is needed.
        """

        self.delayDisplay("Starting the test")

        # Get/create input data

        import SampleData

        registerSampleData()
        inputVolume = SampleData.downloadSample("Label_Studio_Conn1")
        self.delayDisplay("Loaded test data set")

        inputScalarRange = inputVolume.GetImageData().GetScalarRange()
        self.assertEqual(inputScalarRange[0], 0)
        self.assertEqual(inputScalarRange[1], 695)

        outputVolume = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLScalarVolumeNode")
        threshold = 100

        # Test the module logic

        logic = Label_Studio_ConnLogic()

        # Test algorithm with non-inverted threshold
        logic.process(inputVolume, outputVolume, threshold, True)
        outputScalarRange = outputVolume.GetImageData().GetScalarRange()
        self.assertEqual(outputScalarRange[0], inputScalarRange[0])
        self.assertEqual(outputScalarRange[1], threshold)

        # Test algorithm with inverted threshold
        logic.process(inputVolume, outputVolume, threshold, False)
        outputScalarRange = outputVolume.GetImageData().GetScalarRange()
        self.assertEqual(outputScalarRange[0], inputScalarRange[0])
        self.assertEqual(outputScalarRange[1], inputScalarRange[1])

        self.delayDisplay("Test passed")
