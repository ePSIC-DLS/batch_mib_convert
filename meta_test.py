#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 14:10:30 2021

@author: eha56862
"""
import os

root_path = '/dls/e02/data/2021/cm28158-2/Merlin'
processing_path = '/dls/e02/data/2021/cm28158-2/processing/Merlin'

metadata_file_paths = []
mib_file_paths = []
_mib = []
_metadata = []
    
for root, folders, files in os.walk(root_path):
    for file in files:
        if file.endswith('hdf'):
            metadata_file_paths.append(os.path.join(root, file))
        elif file.endswith('mib'):
            mib_file_paths.append(os.path.join(root, file))
_metadata = [f.split('/')[-1].split('.')[0] for f in metadata_file_paths]
_mib = [f.split('/')[-1].split('/')[0].split('.')[0][:-5] for f in mib_file_paths]   
print(list(set(_mib) - set(_metadata)))    

# for root, folders, files in os.walk(processing_path):    
    
#%%
test = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210323_160537/20210323_160537.hdf5'
with h5py.File(test,'r') as f:
    vsource = h5py.VirtualSource(f['Experiments/__unnamed__/data'])
    sh = vsource.shape
    print("4D shape:", sh)
    
#%%
test = '/dls/e02/data/2021/cm28158-2/processing/testing/20210322_165757.hdf'
with h5py.File(test, 'r+', libver='latest') as f:
    # f.create_virtual_dataset(vds_key, layout)
    f['/data/mask'] = h5py.ExternalLink('/dls_sw/e02/medipix_mask/80keV_mask.h5', "/merlin_mask")
    f['metadata']['4D_shape'] = tuple(sh)
#%%

def write_vds(source_h5_path, writing_h5_path, entry_key='Experiments/__unnamed__/data', vds_key = '/data/frames', metadata_path = ''):
    if metadata is None:
        try:
            with h5py.File(source_h5_path,'r') as f:
                vsource = h5py.VirtualSource(f[entry_key])
                sh = vsource.shape
                print("4D shape:", sh)
        except KeyError:
            print('Key provided for the input data file not correct')
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
        dest_path = writing_h5_path
        shutil(src_path, dest_path)
        
        # Open the metadata dest file and add links
        try:
            with h5py.File(source_h5_path,'r') as f:
                vsource = h5py.VirtualSource(f[entry_key])
                sh = vsource.shape
                print("4D shape:", sh)
        except KeyError:
            print('Key provided for the input data file not correct')
            return
    
        layout = h5py.VirtualLayout(shape=tuple((np.prod(sh[:2]), sh[-2], sh[-1])), dtype = np.uint16)
        for i in range(sh[0]):
            for j in range(sh[1]):
                layout[i * sh[1] + j] = vsource[i, j, :, :]
            
        with h5py.File(writing_h5_path, 'r+', libver='latest') as f:
            f.create_virtual_dataset(vds_key, layout)
            f['/data/mask'] = h5py.ExternalLink('/dls_sw/e02/medipix_mask/80keV_mask.h5', "/merlin_mask")
            f['metadata']['4D_shape'] = sh
        
    return
#%%
import h5py
# data_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210322_165757/20210322_165757.hdf5'
# data_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210323_102053/20210323_102053.hdf5'
# data_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210323_135737/20210323_135737.hdf5'
data_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210323_165342/20210323_165342.hdf5'

mask_file = '/dls_sw/e02/medipix_mask/80keV_mask.h5'
mask_key = '/merlin_mask'

with h5py.File(mask_file, 'r') as f:
    mask = f['/merlin_mask'][:]


with h5py.File(data_file, 'r+', libver='latest') as f:
    mask_group = f.create_group('/mask')
    mask_group['data_80kV'] = mask
    # f['/data/mask'] = h5py.ExternalLink('/dls_sw/e02/medipix_mask/80keV_mask.h5', "/merlin_mask")

# d = hs.load(data_file, lazy=True)
#%%
# data_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210322_165757/20210322_165757.hdf5'
# test = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210323_102053/20210323_102053.hdf5'
import h5py
test = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210324_102702/20210324_102702_data.hdf5'
f = h5py.File(test, 'r')

#%%
import h5py
data_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210324_102702/20210324_102702_data.hdf5'
meta_file = '/dls/e02/data/2021/cm28158-2/processing/Merlin/20210324_102702/20210324_102702.hdf'





