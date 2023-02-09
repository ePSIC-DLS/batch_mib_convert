import argparse
import os
from batch_mib_convert.IdentifyPotentialConversions import check_differences
import gc
from batch_mib_convert.mib_dask_import import mib_to_h5stack
from batch_mib_convert.mib_dask_import import parse_hdr
from batch_mib_convert.mib_dask_import import mib_dask_reader
from batch_mib_convert.mib_dask_import import get_mib_depth
import time
import pprint
import hyperspy.api as hs
import pyxem as pxm
import numpy as np
import h5py
import shutil

# ---------------------------------------------------------------
# Use python logging
import logging

# Let the log messages contain some extra information.
formatter = logging.Formatter("%(asctime)s    %(process)5d %(processName)-12s %(threadName)-12s                   %(levelname)-8s %(pathname)s:%(lineno)d %(message)s")
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

# Set the debug log level.
logging.getLogger().setLevel("DEBUG")

# Make a logger for this module.
logger = logging.getLogger(__name__)

# Log the version of code we are running.
#from batch_mib_convert import __version__
#logger.debug(f"{__name__} version {__version__}")

# ---------------------------------------------------------------

# Print the filename where the pxm module is.
logger.debug(f"pxm.__file__ is {pxm.__file__}")

# ---------------------------------------------------------------

hs.preferences.GUIs.warn_if_guis_are_missing = False
hs.preferences.save()

def max_contrast8(d):
    """Rescales contrast of hyperspy Signal2D to 8-bit.
    Parameters
    ----------
    d : hyperspy.signals.Signal2D
        Signal2D object to be rescaled
    Returns
    -------
    d : hyperspy.signals.Signal2D
        Signal2D object following intensity rescaling
    """
    data = d.data
    data = data - data.min()
    if data.max() != 0:
        data = data * (255 / data.max())
    d.data = data
    return d

def change_dtype(d):
    """
    Changes the data type of hs object d to int16
    Parameters:
    -----------
    d : hyperspy.signals.Signal2D
        Signal2D object with dtype float64

    Returns
    -------
    d : hyperspy.signals.Signal2D
        Signal2D object following dtype change to int16
    """
    d = d.data.astype('int16')
    d = hs.signals.Signal2D(d)
    
    return d

def bin_sig(d, bin_fact):
    """
    bins the reshaped 4DSTEMhs object by bin_fact on signal (diffraction) plane
    Parameters:
    ------------
    d: hyperspy.signals.Signal2D - can also be lazy
        reshaped to scanX, scanY | DetX, DetY
        This needs to be computed, i.e. not lazy, to work. If lazy, and binning 
        not aligned with dask chunks raises ValueError 
    Returns:
    -------------
    d_sigbin: binned d in the signal plane
    """
    # figuring out how many pixles to crop before binning
    # we assume the Detx and DetY dimensions are the same
    to_crop = d.axes_manager[-1].size % bin_fact
    d_crop = d.isig[to_crop:,to_crop:]
    try:
        d_sigbin = d_crop.rebin(scale=(1,1,bin_fact,bin_fact))
    except ValueError:
        logger.debug('Rebinning does not align with data dask chunks. Pass non-lazy signal before binning.')
        return
    return d_sigbin

def bin_nav(d, bin_fact):
    """
    bins the reshaped 4DSTEMhs object by bin_fact on navigation (probe scan) plane
    Parameters:
    ------------
    d: hyperspy.signals.Signal2D - can also be lazy
        reshaped to scanX, scanY | DetX, DetY
        This needs to be computed, i.e. not lazy, to work. If lazy, and binning 
        not aligned with dask chunks raises ValueError 
    Returns:
    -------------
    d_navbin: binned d in the signal plane
    """
    # figuring out how many pixles to crop before binning
    # we assume the Detx and DetY dimensions are the same
    to_cropx = d.axes_manager[0].size % bin_fact
    to_cropy = d.axes_manager[1].size % bin_fact
    d_crop = d.inav[to_cropx:,to_cropy:]
    try:
        d_navbin = d_crop.rebin(scale=(bin_fact,bin_fact,1,1))
    except ValueError:
        logger.debug('Rebinning does not align with data dask chunks. Pass non-lazy signal before binning.')
        return
    return d_navbin

def convert(beamline, year, visit, mib_to_convert, folder=None):
    """Convert a set of Merlin/medipix .mib files in a set of time-stamped folders
    into corresponding .hdf5 raw data files, and a series of standard images contained
    within a similar folder structure in the processing folder of the same visit.
    For 4DSTEM files with scan array larger than 300*300 it writes the stack into a h5 file
    first and then loads it lazily and reshapes it.

    Parameters
    ----------
    beamline : str

    year : str

    visit : str

    mib_to_convert : list
        List of MIB files to convert

    folder : str
        kwarg- in case only a specific folder in a visit needs converting, e.g. sample1/dataset1/

    Returns
    -------
        - reshaped 4DSTEM HDF5 file
        - The above file binned by 4 in the navigation plane
        - HSPY, TIFF file and JPG file of incoherent BF reconstruction
        - HSPY, TIFF file and JPG file of sparsed sum of the diffraction patterns
        - An empty txt file is saved to show that the saving of HDF5 files is complete.
    """
    t1 = []
    t2 = []
    t3 = []

    t0 = time.time()
    # Define processing path in which to save outputs
    raw_path = os.path.join('/dls',beamline,'data', year, visit, 'Merlin')
    proc_location = os.path.join('/dls',beamline,'data', year, visit, 'processing', 'Merlin')
    if not os.path.exists(proc_location):
        os.mkdir(proc_location)


    for mib_path in mib_to_convert:
        mib_path = os.path.dirname(mib_path)

        logger.debug('********************************************************')
        logger.debug('Currently active in this directory: %s' % (mib_path.split('/')[-1]))
        # Determine mib files in this folder
        mib_num = 0
        mib_list = []
        for file in os.listdir('/'+ mib_path):
            if file.endswith('mib'):
                mib_num += 1
                mib_list.append(file)
                hdr_info = parse_hdr(os.path.join('/', mib_path, file))
                logger.debug(hdr_info)

        # If there is only 1 mib file in folder load it as a hyperspy Signal2D
        if mib_num == 1:
            # Load the .mib file and determine whether it contains TEM or STEM
            # data based on frame exposure times.
            try:
                logger.debug(f'mib path {mib_path}')

                depth = get_mib_depth(hdr_info, hdr_info['title'] + '.mib')
                logger.debug(f'number of frames: {depth}')
                # Only write the h5 stack for large scan arrays
#                if (depth > 300*300) or (hdr_info['Counter Depth (number)'] > 8):
                if (depth > 300*300):
                    logger.debug('large file 4DSTEM file - first saving the stack into h5 file!')

                    merlin_ind = hdr_info['title'].split('/').index('Merlin')
                    h5_path = proc_location +'/'+ os.path.join(*hdr_info['title'].split('/')[(merlin_ind+1):-1])+ '/' + hdr_info['title'].split('/')[-1] + '.h5'
                    if not os.path.exists(os.path.dirname(h5_path)):
                        os.makedirs(os.path.dirname(h5_path))
                    logger.debug(h5_path)
                    pxm.utils.io_utils.mib_to_h5stack(hdr_info['title'] + '.mib', h5_path)
                    dp = pxm.utils.io_utils.h5stack_to_pxm(h5_path, hdr_info['title'] + '.mib')
                else:
                    logger.debug(f"hdr_info[title].mib is {hdr_info['title'] + '.mib'}")
                    dp = pxm.load_mib(hdr_info['title'] + '.mib')
                logger.debug(dp)
                logger.debug(f"pxm.load_mib returned\n{pprint.pformat(dp.metadata)}")
                dp.compute(show_progressbar = False)
                t1 = time.time()
                if dp.metadata.Signal.signal_type == 'electron_diffraction':
                    STEM_flag = True
                else:
                    STEM_flag = False

            except ValueError:
                logger.debug('file could not be read into an array!')
            # Process single .mib file identified as containing TEM data.
            # This just saves the data as an .hdf5 image stack.
            if STEM_flag is False:
                merlin_ind = hdr_info['title'].split('/').index('Merlin')
                saving_path = proc_location +'/'+ os.path.join(*hdr_info['title'].split('/')[(merlin_ind+1):-1])
                if not os.path.exists(saving_path):
                    os.makedirs(saving_path)
                

                logger.debug(f'saving here: {saving_path}')
                # Calculate summed diffraction pattern
                dp_sum = max_contrast8(dp.sum())
                dp_sum = change_dtype(dp_sum)
                # Save summed diffraction pattern
                dp_sum.save(saving_path + '/' + get_timestamp(mib_path) + '_sum', extension = 'jpg')
                t2 = time.time()
                # Save raw data in .hdf5 format
                dp.save(saving_path + '/' + get_timestamp(mib_path), extension = 'hdf5')
                tmp = []
                np.savetxt(saving_path+'/' + get_timestamp(mib_path) + 'fully_saved', tmp)
                t3 = time.time()
            # Process single .mib file identified as containing STEM data
            # This reshapes to the correct navigation dimensions and
            else:
                # Define save path for STEM data
                if folder:
                    if os.path.split(folder)[0]=='Merlin':
                        folder = folder[(folder.index('/') + 1):]
                    saving_path = os.path.join(proc_location, folder, get_timestamp(mib_path))
                    if not os.path.exists(saving_path):
                        os.makedirs(saving_path)
                
                else:
                    saving_path = os.path.join(proc_location, get_timestamp(mib_path))
                logger.debug(f'saving path: {saving_path}')
                if not os.path.exists(saving_path):
                     os.makedirs(saving_path)


                # Bin by factor of 2 in diffraction and navigation planes
                dp_bin_sig = bin_sig(dp,2)
                dp_bin_nav = bin_nav(dp,2)
                
                # Calculate sum of binned data

                ibf = dp_bin_sig.sum(axis=dp_bin_sig.axes_manager.signal_axes)
                # Rescale contrast of IBF image
                ibf = max_contrast8(ibf)
                ibf = change_dtype(ibf)

                # sum dp image of a subset dataset
                sum_dp_subset = dp_bin_sig.sum()
                sum_dp_subset = max_contrast8(sum_dp_subset)
                sum_dp_subset = change_dtype(sum_dp_subset)

                # save data

                try:
                    logger.debug('Saving average diffraction pattern')
                    file_dp = get_timestamp(mib_path) + '_subset_dp'
                    sum_dp_subset = hs.signals.Signal2D(sum_dp_subset)
                    sum_dp_subset.save(saving_path+'/'+file_dp, extension = 'tiff')
                    sum_dp_subset.save(saving_path+'/'+file_dp, extension = 'jpg')
                    sum_dp_subset.save(saving_path+'/'+file_dp)
                    logger.debug('Saving ibf image')
                    logger.debug(saving_path)
                    ibf = hs.signals.Signal2D(ibf)
                    file_ibf =  get_timestamp(mib_path) + '_ibf'
                    ibf.save(saving_path+'/'+file_ibf, extension = 'tiff')
                    ibf.save(saving_path+'/'+file_ibf, extension = 'jpg')
                    ibf.save(saving_path+'/'+file_ibf)

                    t2 = time.time()
                except:
                    logger.debug('Issue with saving images!')

#                    # Save binned data in .hdf5 file
                logger.debug('Saving binned diffraction data: ' + get_timestamp(mib_path) + '.hdf5')
                dp_bin_sig.save(saving_path+ '/'+'binned_diff_' + get_timestamp(mib_path), extension = 'hdf5')
                logger.debug('Saved binned diffraction data: binned_' + get_timestamp(mib_path) + '.hdf5')
                del dp_bin_sig
                logger.debug('Saving binned navigation data: ' + get_timestamp(mib_path) + '.hdf5')
                dp_bin_nav.save(saving_path+ '/'+'binned_nav_' + get_timestamp(mib_path) , extension = 'hdf5')
                logger.debug('Saved binned navigation data: binned_' + get_timestamp(mib_path) + '.hdf5')
                del dp_bin_nav
#                 # Save complete .hdf5 files
                logger.debug('Saving hdf5 : ' + get_timestamp(mib_path) +'_data.hdf5')
                dp.save(saving_path + '/' + get_timestamp(mib_path) + '_data', extension = 'hdf5')
                logger.debug('Saved hdf5 : ' + get_timestamp(mib_path) +'_data.hdf5')
                tmp = []
                np.savetxt(saving_path+'/' + get_timestamp(mib_path) + 'fully_saved', tmp)
                # Adding mask to hdf5
                
                mask_file = '/dls_sw/e02/medipix_mask/Merlin_12bit_mask.h5'
                mask_key = '/data/mask'
                
                with h5py.File(mask_file, 'r') as f:
                    mask = f[mask_key][:]
                
                
                with h5py.File(saving_path + '/' + get_timestamp(mib_path) + '_data.hdf5', 'r+', libver='latest') as f:
                    f.create_dataset('data/mask', data = mask)

                
                
                t3 = time.time()
                meta_path = find_metadat_file(get_timestamp(mib_path), raw_path)
                logger.debug(f'metadata path: {meta_path}')
                write_vds(saving_path + '/' + get_timestamp(mib_path) + '_data.hdf5', saving_path + '/' + get_timestamp(mib_path) + '_vds.h5', metadata_path=meta_path)

                del dp
                gc.collect()
        # If there are multiple mib files in folder, load them.
        # TODO: Assumes TEM data - needs updating to consider STEM data.
        elif mib_num > 1:

            if folder:
            # find index
                temp1 = mib_path.split('/')
                temp2 = folder.split('/')
                ind = temp1.index(temp2[0])
                saving_path = proc_location +'/'+os.path.join(*mib_path.split('/')[ind:])
            else:
                saving_path = proc_location +'/'+ os.path.join(*mib_path.split('/')[6:])
            if not os.path.exists(saving_path):
                os.makedirs(saving_path)
            logger.debug(f'saving here: {saving_path}')
            for k, file in enumerate(mib_list):
                logger.debug(mib_path)
                logger.debug(file)
                t0 = time.time()
                dp = pxm.load_mib('/' +mib_path + '/'+ file)
                logger.debug(f"pxm.load_mib returned\n{pprint.pformat(dp.metadata)}")
                if dp.metadata.Signal.signal_type == 'electron_diffraction':
                    STEM_flag = True
                else:
                    STEM_flag = False

                dp.compute(show_progressbar = False)

                t1 = time.time()

                if STEM_flag is False:

                    dp_sum = max_contrast8(dp.sum())
                    dp_sum = change_dtype(dp_sum)
                    dp_sum.save(saving_path + '/' +file+'_sum', extension = 'jpg')
                    t2 = time.time()
                    dp.save(saving_path + '/' +file, extension = 'hdf5')
                    t3 = time.time()

         # Print timing information
        if t1 is not None:
            logger.debug(f'time to load data: {int(t1-t0)} seconds')
        if t2 is not None:
            logger.debug(f'time to save last image: {int(t2-t0)} seconds')
        if t3 is not None:
            logger.debug(f'time to save full hdf5: {int(t3-t0)} seconds')
    return


def data_dim(data):
    """
    This function gets the data hyperspy object and outputs the dimensions as string
    to be written into file name ultimately saved.
    input:
        data
    returns:
        data_dim_str: data dimensions as string
    """
    dims = str(data.data.shape)[1:-1].replace(' ','').split(',')
    # 4DSTEM data
    if len(data.data.shape) == 4:
        data_dim_str = '_scan_array_'+dims[0]+'by'+dims[1]+'_diff_plane_'+dims[2]+'by'+dims[3]+'_'
    # stack of images
    if len(data.data.shape) == 3:
        data_dim_str = '_number_of_frames_' + dims[0]+'_detector_plane_'+dims[1]+'by'+dims[1]+'_'
    return data_dim_str


def get_timestamp(mib_path):
    for file in os.listdir(mib_path):
        if file.endswith('mib'):
            time_id = file.split('.')[0]
    return time_id[:-5]

def write_vds(source_h5_path, writing_h5_path, entry_key='Experiments/__unnamed__/data', vds_key = '/data/frames', metadata_path = ''):
    if metadata_path is None:
        try:
            with h5py.File(source_h5_path,'r') as f:
                vsource = h5py.VirtualSource(f[entry_key])
                sh = vsource.shape
                logger.debug(f"4D shape: {sh}")
        except KeyError:
            logger.debug('Key provided for the input data file not correct')
            return
    
        layout = h5py.VirtualLayout(shape=tuple((np.prod(sh[:2]), sh[-2], sh[-1])), dtype = np.uint16)
        for i in range(sh[0]):
            for j in range(sh[1]):
                layout[i * sh[1] + j] = vsource[i, j, :, :]
            
        with h5py.File(writing_h5_path, 'w', libver='latest') as f:
            f.create_virtual_dataset(vds_key, layout)
    else:
        # copy over the metadata file
        src_path = metadata_path
        dest_path = os.path.dirname(writing_h5_path)
        shutil.copy(src_path, dest_path)
        
        # Open the metadata dest file and add links
        try:
            with h5py.File(source_h5_path,'r') as f:
                vsource = h5py.VirtualSource(f[entry_key])
                sh = vsource.shape
                logger.debug(f"4D shape {sh}")
        except KeyError:
            logger.debug('Key provided for the input data file not correct')
            return
    
        layout = h5py.VirtualLayout(shape=tuple((np.prod(sh[:2]), sh[-2], sh[-1])), dtype = np.uint16)
        for i in range(sh[0]):
            for j in range(sh[1]):
                layout[i * sh[1] + j] = vsource[i, j, :, :]
        logger.debug('Adding vds to: ' + os.path.join(dest_path, os.path.basename(metadata_path)))    
        with h5py.File(os.path.join(dest_path, os.path.basename(metadata_path)), 'r+', libver='latest') as f:
            f.create_virtual_dataset(vds_key, layout)
            f['/data/mask'] = h5py.ExternalLink('/dls_sw/e02/medipix_mask/Merlin_12bit_mask.h5', "/data/mask")
            f['metadata']['4D_shape'] = tuple(sh)
        
    return
        
def find_metadat_file(timestamp, acquisition_path):
    metadata_file_paths = []
    mib_file_paths = []
        
    for root, folders, files in os.walk(acquisition_path):
        for file in files:
            if file.endswith('hdf'):
                metadata_file_paths.append(os.path.join(root, file))
            elif file.endswith('mib'):
                mib_file_paths.append(os.path.join(root, file))
    for path in metadata_file_paths:
        if timestamp == path.split('/')[-1].split('.')[0]:
            return path
    logger.debug('No metadata file could be matched.')
    return 
    

if __name__ == "__main__":

    # added by Mihai Duta (7 Dec 2021) -- begin
    from multiprocessing.pool import ThreadPool
    import dask
    dask.config.set(pool=ThreadPool(4))
    # added by Mihai Duta (7 Dec 2021) -- end

    parser = argparse.ArgumentParser()
    parser.add_argument('beamline', help='Beamline name')
    parser.add_argument('year', help='Year')
    parser.add_argument('visit', help='Session visit code')


    parser.add_argument('folder_num', help='passed by scheduler')
    parser.add_argument('-folder', default=None, help='option to add a specific folder within a visit \
                        to look for data, e.g. sample1/dataset1/. If None the assumption would be to look in Merlin folder')
    
    v_help = "Display all debug log messages"
    parser.add_argument("-v", "--verbose", help=v_help, action="store_true",
                        default=False)
    args = parser.parse_args()
    HDF5_dict= check_differences(args.beamline, args.year, args.visit, args.folder)
    to_convert = HDF5_dict['MIB_to_convert']
    logger.debug(f'to convert {to_convert}')

    try:
        if args.folder is not None:
            convert(args.beamline, args.year, args.visit, [to_convert[int(args.folder_num)-1]], folder=args.folder)
        else:

            convert(args.beamline, args.year, args.visit, [to_convert[int(args.folder_num)-1]])

    except Exception as e:
        logger.debug(f'** ERROR processing** \n {e}')

